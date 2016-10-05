require 'logger'
require 'heroku-log-parser'
require_relative './queue_io.rb'
require_relative ENV.fetch("WRITER_LIB", "./writer/s3.rb") # provider of `Writer < WriterBase` singleton

class App

  PREFIX = ENV.fetch("FILTER_PREFIX", "")
  PREFIX_LENGTH = PREFIX.length
  LOG_REQUEST_URI = ENV['LOG_REQUEST_URI']

  def initialize
    Encoding.default_external = Encoding::UTF_8
    @logger = Logger.new(STDOUT)
    @logger.formatter = proc do |severity, datetime, progname, msg|
       "[app #{$$} #{Thread.current.object_id}] #{msg}\n"
    end
    @logger.info "initialized"
  end

  def call(env)
    lines = if LOG_REQUEST_URI
      [env['REQUEST_URI']]
    else
      HerokuLogParser.parse(env['rack.input'].read).collect {|m| m[:message] }
    end

    lines.each do |line|
      next unless line.start_with?(PREFIX)
      line = line[PREFIX_LENGTH..-1].force_encoding('ISO-8859-1')
      line = line.gsub(/original_params=".*}"/, '')
      Writer.instance.write(line) # WRITER_LIB
    end

  rescue Exception
    @logger.error $!
    @logger.error $@

  ensure
    return [200, { 'Content-Length' => '0' }, []]
  end

end
