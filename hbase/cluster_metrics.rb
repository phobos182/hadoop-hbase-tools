#!/usr/bin/ruby

require 'rubygems'
require 'optparse'
require 'mechanize'
require 'time'
require 'socket'

# Options
options = {}

optparse = OptionParser.new do|opts|
  opts.banner = "Usage: cluster_metrics -s [hbase_master] -t [hbase_table] -m [metric]"
  opts.on( '-s', '--server SERVER', 'HBase Master server' ) { |s| options[:server] = s }
  opts.on( '-t', '--table TABLE', 'HBase table on which to collect metrics from' ) { |t| options[:table] = t }
  opts.on( '-m', '--metric METRIC', 'Metric to collect from region' ) { |m| options[:metric] = m }
  opts.on( '-h', '--help', 'Display this screen' ) do
    puts opts
    exit
  end
end

# Parse options
begin
  optparse.parse!
rescue
  puts optparse.banner
  exit 1
end

if options[:server].nil? or options[:metric].nil? or options[:table].nil?
  puts optparse.banner
  exit 1
end

def parse_key_values(text, separator = ',', delimiter = '=')
  result = {}
  begin
    keys = text.split(separator)
    keys.each do |key|
      value = key.chomp.strip
      if value.include?(delimiter)
        key_values = value.split(delimiter)
        key_values.each_slice(2) do |k|
          result[k[0].downcase.to_s] = k[1].to_s if k[1].to_s =~ /^[0-9]+/
        end
      end
    end
  rescue Exception => e
  end
  return result
end

#_ VARIABLES _#
region_servers = []
agent = Mechanize.new
@region_online = 0
@region_servers = []
@result = []
@metric = options[:metric].downcase

page = agent.get('http://' + options[:server] + ':60010/table.jsp?name=' + options[:table])
doc = Nokogiri::HTML(page.body)
table = doc.xpath('/html/body/table[3]')
table.children.collect do |row|
  detail = {}
  [
    [:name, 'td[1]/text()'],
    [:count, 'td[2]/text()']
  ].each do |name, xpath|
    detail[name] = row.at_xpath(xpath).to_s.strip
  end
  @region_servers << detail[:name] unless detail[:name].nil? or detail[:name].empty? or detail[:name].include?('Region Server')
  @region_online += detail[:count].to_i unless detail[:count].include?('Count') or detail[:count].empty? or detail[:count].nil?
end

# Gather metrics from each RS
@region_servers.each do |rs|
  page = agent.get(rs + 'rs-status')
  doc = Nokogiri::HTML(page.body)
  table = doc.xpath('/html/body/table[2]')
  table.children.collect do |row|
    detail = {}
    result = nil
    [
      [:name, 'td[1]/text()'],
      [:start, 'td[2]/text()'],
      [:end, 'td[3]/text()'],
      [:metrics, 'td[4]/text()']
    ].each do |name, xpath|
      detail[name] = row.at_xpath(xpath).to_s.strip
    end
    begin
      metrics = parse_key_values(detail[:metrics])
      rs_match = detail[:name].split(',')[0]
      if rs_match.downcase == options[:table].downcase
        @result << metrics[@metric] if metrics.has_key?(@metric)
      end
    rescue Exception
    end
  end
end

puts 'Regions: ' + @region_online.to_s
puts @result.join(',')
