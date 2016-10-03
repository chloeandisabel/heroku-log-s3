require 'aws'
require 'logger'
require_relative './base.rb'
require 'json'
require 'shellwords'
class Writer < WriterBase

  S3_BUCKET_OBJECTS = AWS::S3.new({
    access_key_id: ENV.fetch('S3_KEY'),
    secret_access_key: ENV.fetch('S3_SECRET'),
  }).buckets[ENV.fetch('S3_BUCKET')].objects

  def generate_filepath
    "new_logs/" + Time.now.utc.strftime(ENV.fetch('STRFTIME', '%Y%m/%d/%H/%M%S.:thread_id.log').gsub(":thread_id", Thread.current.object_id.to_s))
  end

  def stream_to(filepath)
    @logger.info "begin #{filepath}"
    objects = Array.new
    while data = @io.read()
      begin
        pairs = Shellwords.shellwords(data).map{ |s| s.split('=', 2) }.flatten
        hash = Hash[*pairs]
        objects.push hash
      rescue => e
        @logger.error "Error while parsing line:"
        @logger.error e
        @logger.error data
      end
    end
    begin
      write_value = ""
      objects.each do |hash|
        write_value << "#{hash.to_json}\n"
      end
      S3_BUCKET_OBJECTS[filepath].write(write_value)
      @logger.info "end #{filepath}"
    rescue => e
      @logger.error "Error while writting to s3:"
      @logger.error e
    end

  end

end
