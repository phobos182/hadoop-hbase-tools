#!/usr/bin/ruby

require 'rubygems'
require 'optparse'
require 'mechanize'

# OptParse default options
options = {}
options[:prefix] = 'fairscheduler'
options[:server] = 'localhost'

optparse = OptionParser.new do|opts|
  opts.banner = "Usage: scheduler_metrics -s [hadoop_jobtracker]"
  opts.on( '-s', '--server SERVER', 'Hadoop JobTracker' ) { |s| options[:server] = s }
  opts.on( '-k', '--prefix PREFIX', 'Graphite key prefix' ) { |p| options[:prefix] = p }
  opts.on( '-h', '--help', 'Display this screen' ) do
    puts opts
    exit
  end
end

#_ PARSE OPTIONS _#
optparse.parse!

# Check for null server name
if options[:server].nil?
  exit 1
end

#_ VARIABLES _#
pools = []
jobs = []
prefix = options[:prefix]
agent = Mechanize.new

#_ POOL STATS _#
page = agent.get('http://' + options[:server] + ':50030/scheduler?advanced')
doc = Nokogiri::HTML(page.body)

#_ POOL STATS _#
table = doc.xpath('/html/body/table[1]')
table.children.collect do |row|
  detail = {}
  [
    [:pool, 'td[1]/text()'],
    [:jobs, 'td[2]/text()'],
    [:map_tasks, 'td[5]/text()'],
    [:map_fair, 'td[6]/text()'],
    [:reduce_tasks, 'td[9]/text()'],
    [:reduce_fair, 'td[10]/text()']
  ].each do |name, xpath|
    detail[name] = row.at_xpath(xpath).to_s.strip
  end
  # Add pool metrics to array unless the name is empty
  pools << detail unless detail[:pool].empty?
end

#_ RUNNING JOBS STATS _#
table = doc.xpath('/html/body/table[2]')
table.children.collect do |row|
  detail = {}
  [
    [:user, 'td[3]/text()'],
    [:pool, 'td[5]/select/option[@selected]/text()'],
    [:priority, 'td[5]/select/option[@selected]/text()'],
    [:map_progress, 'td[7]/text()'],
    [:map_running, 'td[8]/text()'],
    [:map_fair, 'td[9]/text()'],
    [:map_weight, 'td[10]/text()'],
    [:reduce_progress, 'td[11]/text()'],
    [:reduce_running, 'td[12]/text()'],
    [:reduce_fair, 'td[13]/text()'],
    [:reduce_weight, 'td[14]/text()']
  ].each do |name, xpath|
    detail[name] = row.at_xpath(xpath).to_s.strip
  end
  # Add running jobs to the jobs array if user name is not empty
  jobs << detail unless detail[:user].empty?
end

#_ AGGREGATE POOLS _#
pools.each do |p|
  puts prefix + ".pool.#{p[:pool]}.jobs " + p[:jobs]
  puts prefix + ".pool.#{p[:pool]}.maps " + p[:map_tasks]
  puts prefix + ".pool.#{p[:pool]}.reduces " + p[:reduce_tasks]
end

#_ AGGREGATE RUNNING JOBS _#
#_ Get a list of unique pool names _#
pool_names = jobs.collect { |c| c[:pool] }.uniq

#_ Iterate over pool names, aggregate running jobs by pool _#
pool_names.each do |p|
  j = jobs.select { |i| i[:pool] == p }
  maps_running = j.inject(0) { |sum, job| sum + job[:map_running].to_i }
  maps_weight = j.inject(0) { |sum, job| sum + job[:map_weight].to_i }
  map_progress = j.inject({ :complete => 0, :total => 0}) do |progress, job|
    job_progress = job[:map_progress].split('/')
    progress[:complete] += job_progress[0].strip.chomp.to_i
    progress[:total] += job_progress[1].strip.chomp.to_i
    progress
  end
  reduce_progress = j.inject({ :complete => 0, :total => 0}) do |progress, job|
    job_progress = job[:reduce_progress].split('/')
    progress[:complete] += job_progress[0].strip.chomp.to_i
    progress[:total] += job_progress[1].strip.chomp.to_i
    progress
  end
  reudces_running = j.inject(0) { |sum, job| sum + job[:reduce_running].to_i }
  reudces_weight = j.inject(0) { |sum, job| sum + job[:reduce_weight].to_i }
  puts prefix + ".pool.#{p}.maps_running " + maps_running.to_s
  puts prefix + ".pool.#{p}.maps_weight "+ maps_weight.to_s
  puts prefix + ".pool.#{p}.maps_complete " + map_progress[:complete].to_s
  puts prefix + ".pool.#{p}.maps_total " + map_progress[:total].to_s
  puts prefix + ".pool.#{p}.reduces_running " + reudces_running.to_s
  puts prefix + ".pool.#{p}.reduces_weight " + reudces_weight.to_s
  puts prefix + ".pool.#{p}.reduces_complete " + reduce_progress[:complete].to_s
  puts prefix + ".pool.#{p}.reduces_total " + reduce_progress[:total].to_s
end
