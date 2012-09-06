#!/usr/bin/ruby

require 'rubygems'
require 'optparse'
require 'mechanize'
require 'time'
require 'socket'

# Options
options = {}

optparse = OptionParser.new do|opts|
  opts.banner = "Usage: region_metric -s [hbase_master] -t [hbase_table]"
  opts.on( '-s', '--server SERVER', 'HBase Master server' ) { |s| options[:server] = s }
  opts.on( '-t', '--table TABLE', 'HBase table to balance' ) { |s| options[:table] = s }
  opts.on( '-h', '--help', 'Display this screen' ) do
    puts opts
    exit
  end
end

# Parse options
optparse.parse!

if options[:server].nil? or options[:table].nil?
  exit 1
end

# RS Array
rs = {}
low_count = {}
remainder = {}
high_count = []
rs_startcode = {}
region_count = 0
move_operations = {}

agent = Mechanize.new
# Load the page with Mechanize
page = agent.get('http://' + options[:server] + ':60010/master-status')
# Search for all tables
table = page.search('//table')
region_servers = []
table[3].search('tr').each do |row|
  list = nil
  server_name = nil

  data = row.search('td')
  begin
    list = data.children.inner_text.split(',')[0]
    rs_holder = data.children.inner_text.split(' ')[0].slice(0...-3)
    rs_startcode[rs_holder.split(',')[0]] = rs_holder
  rescue
  end

  server_name = list unless list.nil? or list.include?('servers:')
  if not server_name.nil?
    if not rs.has_key?(server_name)
      rs[server_name] = { :regions => [], :count => 0 }
    end
  end
end

# Gather info from Table
page = agent.get('http://' + options[:server] + ':60010/table.jsp?name=' + options[:table])
table = page.search('/html/body/table[2]')
doc = Nokogiri::HTML(page.body)
rows = doc.xpath('//table[1]/tbody/tr')
table.children.collect do |row|
  detail = {}
  [
    [:region, 'td[1]/text()'],
    [:server, 'td[2]/a/text()'],
    [:start, 'td[3]/text()'],
    [:end, 'td[4]/text()'],
    [:requests, 'td[5]/text()']
  ].each do |name, xpath|
    detail[name] = row.at_xpath(xpath).to_s.strip
  end
  if not detail[:server] == ''
    server = detail[:server].split(':')[0]
    if not server.nil?
      rs[server] = {} unless rs.has_key?(server)
      region_count += 1
      rs[server][:regions] << detail[:region]
      rs[server][:count] += 1
    end
  end
end

puts '** HBASE STATS **'
puts 'Servers: ' + rs.length.to_s
puts options[:table] + ' regions: ' + region_count.to_s
puts '** REGION ASSIGNMENTS **'
avg_region = region_count.to_f / rs.length.to_f
mark_upper = avg_region.ceil
mark_lower = avg_region.floor
puts 'Upper Bound: ' + mark_upper.to_s
puts 'Lower Bound: ' + mark_lower.to_s

rs.each do |r|
  num_regions = r[1][:regions].length
  region_name = r[0]
  case
    when num_regions >= mark_upper
      high_count << r
    when num_regions < mark_lower
      low_count[region_name] = num_regions
    when num_regions = mark_lower
      remainder[region_name] = num_regions
  end
end

puts '** TABLE STATS **'
puts 'OK:   ' + remainder.length.to_s
puts 'LOW:  ' + low_count.length.to_s
puts 'HIGH: ' + high_count.length.to_s

queue = Queue.new

puts '** POPULATING QUEUE **'

# Move elements from the higher queues to the lower queues
high_count.each do |r|
  move_count = 0
  # Calculate number of regions to move
  move_regions = (mark_lower - r[1][:regions].length).abs 
  puts 'Moving ' + move_regions.to_s + ' regions from ' + r[0] 
  r[1][:regions].each do |region|
    # Region to move
    puts '  ' + region
    queue << region
    move_count += 1
    # Add region to be moved to the array.
    break if move_count == move_regions
  end
end

puts '** CONSUMING QUEUE **'
# Now consume the queue for each system in the lower count
low_count.each do |r|
  counter = 0
  move_count = (mark_lower - r[1]).abs
  puts move_count.to_s + ' move operations to ' + r[0] + ' to make ' + (move_count + r[1]).to_s + ' total regions'
  (0...move_count).each do
    op = queue.pop
    puts '  Moving ' + op + ' to ' + r[0]
    move_operations[r[0]] = [] unless move_operations.has_key?(r[0]) 
    move_operations[r[0]] << op
  end
end

# Everybody is now at the lower bound, but we may have some remainders. 
# Go through ALL systems, and hand out regions 1-by-1 until we are
# out of regions.
puts '** ' + queue.length.to_s + ' REMAINDERS **'
# Add low_count array to remainder array
remainder_count = remainder.merge(low_count)
# Add high count array to remainder array
high_count.each do |r,v|
  remainder_count[r] = 1
end

# If no remainders (Regions / RegionServers divides perfectly),
# Then no need to assign them.
until  queue.length == 0
  remainder_count.each do |region,value|
    begin
      break if queue.length == 0
      op = queue.pop
      puts '  Moving ' + op + ' to ' + region
      move_operations[region] = [] unless move_operations.has_key?(region)
      move_operations[region] << op
    end
  end
end

puts '** HBASE SHELL COMMANDS **'
move_operations.each do |server, region|
  region.each do |r|
    puts "move '#{r.split('.')[-1]}', '#{rs_startcode[server]}'"
  end
end
