
class HadoopJMX
  require 'json'
  require 'net/http'
  require 'uri'
  require 'timeout'

  attr_reader :jmx
  attr_accessor :timeout, :port, :uri, :server

  def initialize(server=nil, port=nil, uri='/jmx', timeout=5)
    # Set options
    @server = server
    @timeout = timeout
    @port = port
    @uri = uri

    # Initially populate metrics unless empty constructor
    self.refresh() unless @server.nil?
  end

  def refresh()
    # Create an URI request object from string
    raise "Please specify a host or a port" if (@server.nil? || @port.nil? || @uri.nil?)
    begin
      @request = URI.parse('http://' + @server + ':' + @port + @uri)
    rescue Exception => e
      puts 'Malformed URI ' + e
    end

    # Get JMX results
    @response = self.get_request(@request)

    # Parse response as Json
    @jmx = parse_json(@response)
  end

  def metrics()
    self.refresh
    return @jmx
  end

  def get_request(req)
    response = nil
    output = nil
    Timeout::timeout(@timeout) do |length|
      begin
        output = Net::HTTP.get_response(req)
      rescue Exception => e
        puts 'Exception requesting ' + req.to_s + ' ' + e
      end
    end
    response = output.body if output.is_a?(Net::HTTPSuccess)
    return response
  end

  def parse_json(json)
    begin
      result = JSON.parse(json)
    rescue Exception => e
      puts "Exception parsing JSON " + e.message
    end
    return result 
  end

  def find_mbean(mbean)
    @mbean = nil

    # Refresh metrics if JMX tree is blank
    self.refresh if @jmx.nil?

    # Walk through the hash recursively finding the key
    # This will set @mbean to nil, or my result
    recurse_find(mbean, @jmx)

    return @mbean
  end

  def recurse_find(key, item)
    # Keep recursing if item can be enumerated
    if item.class == Hash || item.class == Array
      item.each do |k,v|
        if k.include?(key)
          # Set @mbean to the value of the Key found
          # Try to parse the response as JSON. Otherwise return key value
          begin
            @mbean = JSON.parse(k[key])
          rescue Exception => e
            @mbean = k[key]
          end
          # Return first match. Stop parsing
          break
        else
          self.recurse_find(key, v)
        end
      end
    end
  end
end
