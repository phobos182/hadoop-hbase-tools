#!/usr/bin/ruby

require 'rubygems'
require 'optparse'
require 'mechanize'
require 'socket'
require 'json'

#_ FUNCTION _#
#_ RUN HBASE SHELL CMDS _#
def hbase_shell(cmd)
  puts 'Shell: ' + cmd.inspect
  cmd_failed = false
  hbase_shell = %x[echo #{cmd.inspect} | hbase shell 2>/dev/null]
  cmd_failed = true if hbase_shell.downcase.include?('error')
  raise "Drop command failed\n#{hbase_shell}" if cmd_failed
end

def hadoop_fs(cmd)
  puts 'Hadoop FS: ' + cmd.inspect
  cmd_failed = false
  hadoop_shell = %x[hadoop fs #{cmd}]
  code = $?.exitstatus
  raise "Hadoop FS command failed\n#{hadoop_shell}" if code != 0
end

def format_bulkload
  # Clean staging directory
  begin
    hadoop_fs("-rmr /tmp/import")
  rescue
  end
  # Move files for each family to the staging directory
  @hbase_schema[FAMILIES].each do |f|
    hadoop_fs("-mkdir /tmp/import/#{f['NAME']}")
    hadoop_fs("-mv /tmp/#{@table}/*/#{f['NAME']}/* /tmp/import/#{f['NAME']}/")
  end
end

def bulkload
  jar = %x[ls /usr/lib/hbase/*.jar|grep -v test].chomp
  raise 'Could not find HBase jar' if jar.empty? or jar.nil?
  bulk_cmd = %x[export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/usr/lib/hbase/lib/*;hadoop jar #{jar} completebulkload /tmp/import #{@table}]
  code = $?.exitstatus
  raise "Bulk load failed\n#{bulk_cmd}" if code != 0
end

#_ FUNCTION _#
#_ HADOOP DISTCP - COPY HBASE TABLE TO TMP _#
def distcp
  # Clean staging directory
  begin
    hadoop_fs("-rmr /tmp/#{@table}")
  rescue
  end
  cmd_failed = false
  hadoop_shell = %x[hadoop distcp -i hdfs://#{@server}:8020/hbase/#{@table} hdfs://#{@namenode}:8020/tmp]
  cmd_failed = true if hadoop_shell.downcase.include?('error') or hadoop_shell.downcase.include?('failed') or hadoop_shell.downcase.include?('killed')
  raise "DistCP failed\n#{hbase_shell}" if cmd_failed
end

# OptParse default options
options = {}
optparse = OptionParser.new do|opts|
  opts.banner = "Usage: copy_table -s [hbase master to copy from] -t [hbase table]\nExample: copy_table -s jobs-dev-hnn -t user_score"
  opts.on( '-s', '--server SERVER', 'HBase server to copy from' ) { |s| options[:server] = s }
  opts.on( '-t', '--table TABLE', 'HBase table' ) { |p| options[:table] = p }
  opts.on( '-h', '--help', 'Display this screen' ) do
    puts opts
    exit
  end
end

#_ PARSE OPTIONS _#
optparse.parse!

# Check for null server options
if options[:table].nil? or options[:server].nil?
  puts optparse.banner
  exit 1
end

#_ VARIABLES _#
@hostname = Socket.gethostbyname(Socket.gethostname).first
# Assume namenode is this hostname without the last char.
# Ex: jobs-dev-hnn2 = jobs-dev-hnn
if @hostname.split('.')[0][-4...-1] == 'hnn'
  @namenode = @hostname.split('.')[0][0...-1]
else
  @namenode = @hostname
end
@table = options[:table]
@server = options[:server]
@family = []
@hbase_schema = {}
agent = Mechanize.new
hbase_table = ''
hbase_splits = []
TTL = 'TTL'
NAME = 'NAME'
FAMILIES = 'FAMILIES'
VERSIONS = 'VERSIONS'
IN_MEMORY = 'IN_MEMORY'
BLOCKSIZE = 'BLOCKSIZE'
BLOCKCACHE = 'BLOCKCACHE'
COMPRESSION = 'COMPRESSION'
BLOOMFILTER = 'BLOOMFILTER'
MIN_VERSIONS = 'MIN_VERSIONS'
REPLICATION_SCOPE = 'REPLICATION_SCOPE'

#_ MASTER INFO _#
page = agent.get('http://' + @server + ':60010/master-status')
doc = Nokogiri::HTML(page.body)
table = doc.xpath('/html/body/table[3]')

raise "Count not get master status from HBase #{@server}" if table.empty?

table.children.collect do |row|
  detail = {}
  [
    [:table, 'td[1]/a/text()'],
    [:schema, 'td[2]/text()']
  ].each do |name, xpath|
    detail[name] = row.at_xpath(xpath).to_s.strip
  end
  # If HBase table name matches supplied arguments, then add to hash
  hbase_table = detail[:schema] if detail[:table] == @table
end

# Exit if no table found
raise "Count not find table on HBase master #{@server}" if hbase_table.empty?

# Mechanize converts escaped characters back to ascii 
hbase_table = hbase_table.gsub('&gt;', '>').gsub('&lt;', '<')
# I know this is dangerous. But it's easy to convert the result to a hash object with eval
@hbase_schema = eval(hbase_table)

# If eval failed, then abort
raise 'Could not convert table schema to hash' if not @hbase_schema.class.to_s == 'Hash'

#_ REGION SPLITS _#
# Lets query the Master for region start keys for the splits file
page_splits = agent.get('http://' + @server + ':60010/table.jsp?name=' + @table)
doc = Nokogiri::HTML(page_splits.body)
table = doc.xpath('/html/body/table[2]')
table.children.collect do |row|
  [
    [:key, 'td[3]/text()']
  ].each do |name, xpath|
    hbase_splits << row.at_xpath(xpath).to_s.strip
  end
end

# Remove empty elements from splits array
hbase_splits.reject! { |c| c.empty? }

# Populate the family array with the hashes for each CF
@hbase_schema[FAMILIES].each { |f| @family << f.inspect }

# Construct create table string
# If no splits found, then create table without them specified
if hbase_splits.empty?
  create_string = "create '#{@table}', #{@family.join(',')}"
else
  create_string = "create '#{@table}', #{@family.join(',')}, SPLITS => ['#{hbase_splits.join("','")}']"
end

# Check to see if the table exists on the target system
# If the table is not found, we receive a 500 with a TableNotFoundException.
# In this case, no need to drop the table. If the table is found we get 200
should_drop = false
begin
  page_status = agent.get('http://' + @hostname + ':60010/table.jsp?name=' + @table)
  should_drop = true if page_status.code == '200'
rescue Mechanize::ResponseCodeError => exception
  # If we do not receive 200, or 500 response code then abort.
  if exception.response_code == '500'
    # If our exception is not TableNotFoundException, then abort
    if exception.page.body.include?('TableNotFoundException')
      should_drop = false
    else
      raise
    end
  else
    raise
  end
end

# If table found on local system, then drop it
if should_drop
  hbase_shell("disable '#{@table}'")
  hbase_shell("drop '#{@table}'")
end

# Create table
hbase_shell(create_string)

# Table has been created, now DistCP the data from the target 
# system to HDFS
distcp

# Now get this in the format required for bulk import.
# All CF have to be in one directory, with just the HFiles
format_bulkload

# Perform bulkload operation
bulkload

