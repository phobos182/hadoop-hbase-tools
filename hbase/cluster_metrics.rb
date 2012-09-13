#!/usr/bin/ruby

require 'rubygems'
require 'optparse'
require 'mechanize'
require 'time'
require 'socket'

# Options
options = {}
options[:file] = false

optparse = OptionParser.new do|opts|
  opts.banner = "Usage: cluster_metrics -s [hbase_master] -t [hbase_table] -m [metric]"
  opts.on( '-s', '--server SERVER', 'HBase Master server' ) { |s| options[:server] = s }
  opts.on( '-f', '--file FILE', 'Output to CSV File' ) { |f| options[:file] = f }
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

if options[:server].nil?
  puts optparse.banner
  exit 1
end

if options[:file]
  @f = File.open(options[:file],'w')
end

def std_out(string)
  if @f
    @f.puts(string)
  else
    puts string
  end
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
agent = Mechanize.new
@region_servers = []
@result = {}

page = agent.get('http://' + options[:server] + ':60010/master-status')
doc = Nokogiri::HTML(page.body)
table = doc.xpath('/html/body/table[4]')
table.children.collect do |row|
  detail = {}
  [
    [:name, 'td[1]/a/text()']
  ].each do |name, xpath|
    detail[name] = row.at_xpath(xpath).to_s.strip
  end
  rs = detail[:name].split(',')[0]
  @region_servers << rs unless rs.nil? or rs.empty?
end

#_ REGIONSERVER INFO _#
@region_servers.each do |rs|
  page = agent.get('http://' + rs + ':60030/rs-status')
  doc = Nokogiri::HTML(page.body)
  regionserver_table = doc.xpath("/html/body/table[@id='attributes_table']")
  tasks_running = doc.xpath("/html/body/table[2]/tr[1]/th[1]/text()")
  # If compaction / split tasks are running. Shift the table
  # number down by 1.
  tasks_running.empty? or tasks_running.to_s.include?('Region') ? table_num = 2 : table_num = 3
  region_table = doc.xpath("/html/body/table[#{table_num}]")

  raise "Count not get regionserver status from HBase #{rs}" if regionserver_table.empty? or region_table.empty?

  region_table.children.collect do |row|
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
      table = detail[:name].split(',')[0]
      metric_hash = parse_key_values(detail[:metrics])
      region_md5 = detail[:name].split(',')[2].split('.')[1]
      @result[region_md5] = { :md5 => region_md5,  :server => rs, :metrics => metric_hash, :table => table }
    rescue Exception
    end
  end
end

#_ CSV HEADERS _#
# Build a list of all hash values to include in the CSV.
# Will be used as a 'skeleton' to merge with other hash
# values.
headers = {}
@result.inject(headers) do |result,k|
  k[1].each do |k1,v1|
    if not k1.to_s.include?('metric')
      result.has_key?(k1) ? result[k1] += 1 : result[k1] = 0
    end
    if v1.is_a?(Hash)
      v1.each do |k2,v2|
        result.has_key?(k2) ? result[k2] += 1 : result[k2] = 0
      end
    end
  end
  result
end

#_ REMOVE UNIQUES _#
# Remove any value which is not present
# in all regions. Not all regions have
# compression, etc.. 
max = headers.values.max.to_i
excluded = headers.collect {|key, value| key if value.to_i < max } 
excluded.delete_if {|x| x == nil}

#_ SORT HEADERS _#
headers.delete_if {|key, value| value.to_i < max } 
headers = headers.keys.collect { |i| i.to_s }.sort

#_ OUTPUT CSV HEADER _#
std_out(headers.join(','))

@result.each do |r,hash|
  output = {}
  sorted = []
  ostring = []
  hash.each do |k,v|
    if v.is_a?(Hash)
      v.each do |k1,v1|
        output.merge!({ k1.to_s => v1})
      end
    else
      output.merge!({ k.to_s => v})
    end
  end
  sorted = output.keys.sort
  sorted.delete_if {|x| excluded.include?(x) }
  # Test to make sure the sorted headers match output
  # keys. Otherwise do not print the output
  break unless "#{headers.join(',')}" == "#{sorted.join(',')}"

  # Collect metrics in correct sorted order
  sorted.each do |m|
    ostring << output[m] unless excluded.include?(m)   
  end
  # Display CSV
  std_out(ostring.join(','))
end
