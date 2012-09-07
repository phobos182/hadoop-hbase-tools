#!/usr/bin/ruby

require 'rubygems'
require 'optparse'
require 'mechanize'
require 'time'
require 'hadoopjmx'
require 'socket'
require 'thread'

# Options
options = {}

#_ FUNCTION _#
# Print banner and exit
def print_banner(optparse)
    puts optparse.banner
    exit 3
end

#_ FUNCTION _#
# Compare value against warning / critical thresholds
# Return the Nagios style exide code for use.
def compare(value, warn, crit)
  return 2, "CRITICAL" if value >= crit
  return 1, "WARN" if value >= warn
  return 0, "OK"
end

#_ DEFAULT OPTIONS _#
options[:method] = 'sum'
options[:thresh] = 99

optparse = OptionParser.new do|opts|
  opts.banner = "Usage: hbase_nagios -s [hbase_master] -m (sum|max|avg|pct%) -w [warn] -c [crit] -p [JMX Key]\n  Example: hbase_nagios -s hbase1-nn1 -m pct75 -w 50 -c 200 -p sizeOfLogQueue"
  opts.on( '-s', '--server SERVER', 'HBase Master server' ) { |s| options[:server] = s }
  opts.on( '-w', '--warn WARN', 'Warning threshold' ) { |w| options[:warn] = w }
  opts.on( '-c', '--crit CRIT', 'Critical threshold' ) { |c| options[:crit] = c }
  opts.on( '-p', '--path PATH', 'String match for JMX Mbean key (Ex: sizeOfLogQueue)' ) { |p| options[:path] = p }
  opts.on( '-m', '--method METHOD', 'Method of aggregating metrics (default: sum (sum|max|avg|pct%)' ) { |m| options[:method] = m }
  opts.on( '-t', '--threshold THRESH', '% of nodes that have to report (not timeout) to be a valid result (default: 99%)' ) { |t| options[:thresh] = t }
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

# Look for required arguments
print_banner(optparse) unless options.has_key?(:server)
print_banner(optparse) unless options.has_key?(:warn)
print_banner(optparse) unless options.has_key?(:crit)
print_banner(optparse) unless options.has_key?(:path)

#_ VARIABLES _#
agent = Mechanize.new
@region_servers = []
@result = []
@server = options[:server]
@path = options[:path]
@warn = options[:warn].to_i
@crit = options[:crit].to_i
@thresh = options[:thresh].to_s.gsub('%', '').to_i
@agg = options[:method]
@threads = []
@metric = nil
@confidence = nil
mutex = Mutex.new

#_ HBASE MASTER _#
# Collect RegionServers from Master
agent = Mechanize.new
page = agent.get('http://' + @server + ':60010/master-status')
doc = Nokogiri::HTML(page.body)
table = doc.xpath('/html/body/table[4]')
table.children.collect do |row|
  detail = {}
  server_name = nil
  server_port = nil
  [
    [:server, 'td[1]/a/text()']
  ].each do |name, xpath|
    detail[name] = row.at_xpath(xpath).to_s.strip
  end
  server_array = detail[:server].split(',')
  server_name = server_array[0]
  server_port = server_array[1]
  @region_servers << { :server => server_name } unless server_name.nil? or server_port.nil?
end

#_ METRICS _#
# Iterate over each RegionServer. Gather metrics
# Spawn in own thread for performance. Locks on
# updating result
@region_servers.each do |rs|
  @threads << Thread.new do
    jmx = HadoopJMX.new
    jmx.server = rs[:server]
    jmx.port = '60030'
    begin
      answer = jmx.find_mbean(@path)
    rescue Exception
      answer = nil
    end
    mutex.synchronize do
      @result << answer
    end
  end
end

# Wait for all threads to finish
@threads.each do |t|
  t.join
end

#_ AGGREGATION _#
# Specified (Ex: sum, avg, pct95, max)
case @agg
  when 'sum'
    @metric = @result.inject(0) { |sum,el| sum += el.to_i }
  when 'avg'
    @metric = @result.inject(0.0) { |sum,el| sum += el.to_i } / @result.size
  when /pct/
    tile = @agg.gsub('pct', '').to_f / 100.0
    pos = (tile * @result.size.to_f).round
    @metric = @result.sort[pos]
  when 'max'
    @metric = @result.max.to_i
end

#_ TIMEOUT CALCULATION _#
# Look for instances of request timeouts.
# Add them up, compare to % threshold for
# UNKNOWN status.
timedout = @result.inject(0.0) do |sum,el|
  sum += 1 if el.nil?
  sum
end

@confidence = (((timedout.to_f / @result.size.to_f) * 100).round - 100).abs

#_ COMPARE & EXIT _#
# Compare results to warn / crit. Exit with string + status
exit_code, exit_status = compare(@metric, @warn, @crit)
if @confidence <= @thresh
  puts "UNKNOWN | metric=#{@metric} confidence=#{@confidence}% reported=#{(timedout - @result.size).abs.to_i}/#{@result.size}"
  exit 3
else
  puts "#{exit_status} | metric=#{@metric} confidence=#{@confidence}% reported=#{(timedout - @result.size).abs.to_i}/#{@result.size}"
  exit exit_code
end
