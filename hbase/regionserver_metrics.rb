#!/usr/bin/ruby

require 'rubygems'
require 'optparse'
require 'mechanize'
require 'time'
require 'socket'

#_ VARIABLES _#
# OptParse hash
options = {}
# Get HostName
hostname = Socket.gethostbyname(Socket.gethostname).first.split(".")[0]
# Hash which will hold per table metrics
@table_metrics = {}
# Keys which should be converted from Nanoseconds to Milliseconds
@convert_keys = [ 'deleterequestlatencymean', 'deleterequestlatencymedian', 'deleterequestlatency75th', 'deleterequestlatency95th', 'deleterequestlatency99th', 'deleterequestlatency999th', 'getrequestlatencymean', 'getrequestlatency75th', 'getrequestlatency75th', 'getrequestlatency95th', 'getrequestlatency99th', 'getrequestlatency999th', 'putrequestlatencymean', 'putrequestlatency999th', 'fsreadlatencyhistogrammean', 'fsreadlatencyhistogrammedian', 'fsreadlatencyhistogram75th', 'fsreadlatencyhistogram95th', 'fsreadlatencyhistogram99th', 'fsreadlatencyhistogram999th', 'fswritelatencyhistogrammean', 'fswritelatencyhistogrammedian', 'fswritelatencyhistogram75th', 'fswritelatencyhistogram95th', 'fswritelatencyhistogram99th', 'fswritelatencyhistogram999th' ]
# Exclude these keys from metrics collection.
@exclude_keys = [ 'compactionprogresspct', 'totalcompactingkvs', 'currentcompactedkvs']

optparse = OptionParser.new do|opts|
  opts.banner = "Usage: hbase_metrics -s [server] -p [prefix]"
  options[:server] = hostname
  options[:prefix] = "hbase.regionserver"
  options[:table] = false
  options[:verbose] = false
  opts.on( '-s', '--server SERVER', 'HBase RegionServer (default: <hostname>)' ) { |s| options[:server] = s }
  opts.on( '-p', '--prefix PREFIX', 'Graphite key prefix (default: hbase.regionserver.<hostname>)' ) { |p| options[:prefix] = p }
  opts.on( '-t', '--table TABLE', 'Print table metrics per region. (all|table_name)' ) { |t| options[:table] = t }
  opts.on( '-v', '--verbose', 'Send metrics 0 as value. (default: false)' ) { |t| options[:verbose] = true }
  opts.on( '-h', '--help', 'Display this screen' ) do
    puts opts
    exit
  end
end

# Parse options
optparse.parse!

#_ OPTPARSE VARIABLES _#
@server = options[:server]
@key_name = options[:prefix] + '.' + options[:server]
@table = options[:table]
@verbose = options[:verbose]

#_ FUNCTION _#
# Graphite: Print KeyValues in Graphite format
def graphite(prefix, key, value)
  # Curerent time
  epoch = Time.now.to_i

  # Test for value which is 0. Print if options[:verbose]
  if value == 0
    puts prefix + "." + key + " " + value.to_s + " " + epoch.to_s if @verbose
  else
    puts prefix + "." + key + " " + value.to_s + " " + epoch.to_s
  end
end

#_ FUNCTION _#
# Parse Key Values: Send in a string with a seperator
# and a delimiter. Will turn into a hash of KeyValue
# pairs:
# Ex: mykey1=1, mykey2=2 (Result: { mykey1 => 1, mykey2 => 2 })
def parse_key_values(text, separator = ',', delimiter = '=')
  result = {}
  begin
    keys = text.split(separator)
    keys.each do |key|
      value = key.chomp.strip
      if value.include?(delimiter)
        key_values = value.split(delimiter)
        key_values.each_slice(2) do |k|
          # Add key and value to hash if value begins with an integer
          result[k[0].downcase.to_s] = k[1].to_s if k[1].to_s =~ /^[0-9]+/
        end
      end
    end
  rescue Exception => e
  end
  return result
end

#_ MAIN _#

#_ VARIABLES _#
agent = Mechanize.new

#_ REGIONSERVER INFO _#
page = agent.get('http://' + @server + ':60030/rs-status')
doc = Nokogiri::HTML(page.body)
regionserver_table = doc.xpath("/html/body/table[@id='attributes_table']")
tasks_running = doc.xpath("/html/body/table[2]/tr[1]/th[1]/text()")
# If compaction / split tasks are running. Shift the table
# number down by 1.
tasks_running.empty? or tasks_running.to_s.include?('Region') ? table_num = 2 : table_num = 3
region_table = doc.xpath("/html/body/table[#{table_num}]")

raise "Count not get regionserver status from HBase #{@server}" if regionserver_table.empty? or region_table.empty?

#_ REGIONSERVER METRICS _#
regionserver_table.children.collect do |row|
  detail = {}
  result = nil
  [
    [:name, 'td[1]/text()'],
    [:value, 'td[2]/text()'],
    [:description, 'td[3]/text()']
  ].each do |name, xpath|
    detail[name] = row.at_xpath(xpath).to_s.strip
  end

  if not detail[:name].empty?
    # Check to see if this is the metrics row
    if detail[:name].chomp.include?('Metrics')
      parse_key_values(detail[:value]).each do |k,v|
        # Hack - If key contains latency convert from Nanoseconds => Milliseconds
        v = (v.to_i / 1000000.0).to_s if @convert_keys.any? {|i| i.include?(k) }
        # Some HBase metrics have '%'. Remove them
        v.gsub!('%', '') if v.include?('%')
        # Print graphite key
        graphite(@key_name, k, v)
      end
    end
  end
end

#_ REGION METRICS _#
#_ EXIT HERE IF NO TABLE SPECIFIED _#
exit 0 unless @table

region_table.children.collect do |row|
  detail = {}
  [
    [:region, 'td[1]/text()'],
    [:start, 'td[2]/text()'],
    [:end, 'td[3]/text()'],
    [:metrics, 'td[4]/text()']
  ].each do |name, xpath|
    detail[name] = row.at_xpath(xpath).to_s.strip
  end
  if not detail[:region].empty?
    # Collect region name information. Split into parts
    region_array = detail[:region].split(',')
    # First part is the table name
    table_name = region_array[0]
    # Collect region MD5 unless -ROOT-, or .META. as they do not contain it.
    region_md5 = region_array[2].split('.')[1] unless table_name.include?('-ROOT-') or table_name.include?('.META.')

    # Parse region metrics if table matched, or all specified
    if @table == 'all' or table_name.downcase == @table.downcase
      metrics = parse_key_values(detail[:metrics])
      metrics.each do |k,v|
        # Some HBase metrics have '%'. Remove them
        v.gsub!('%', '') if v.include?('%')

        #_ TABLE ROLLUP _#
        # Create table entry in hash if it does not exist
        @table_metrics[table_name] = {} unless @table_metrics.has_key?(table_name)
        # Add value of key entry to table_metrics[table] hash if it exists. Otherwise, initialize 
        # the hash with the current value
        @table_metrics[table_name].has_key?(k) ? @table_metrics[table_name][k] += v.to_i : @table_metrics[table_name][k] = v.to_i
      end

      # Add the number of regions we have seen as a new entry to this hash.
      # Helps us determine averages since we are rolling up values.
      @table_metrics[table_name].has_key?('onlineregions') ? @table_metrics[table_name]['onlineregions'] += 1 : @table_metrics[table_name]['onlineregions'] = 1
    end

  end
end

#_ Print table rollup information 
@table_metrics.each do |table,hash|
  hash.each do |k, v|
    graphite(@key_name + "." + table, k, v)
  end
end
