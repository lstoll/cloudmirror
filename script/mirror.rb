#!/usr/bin/env ruby

Thread.abort_on_exception = true

require 'thread'
require 'fog'
require 'dotenv'

Dotenv.load

# Ensure stuff is set
req_params = %w(S3_KEY S3_SECRET S3_BUCKET RS_USER RS_KEY RS_CONTAINER)
unless req_params.all? {|i| ENV[i]}
  raise "Environment needs #{req_params.join(' ')}"
  exit 1
end

# Set up threads and variables.
in_dir = ENV['S3_BUCKET']
out_dir = ENV['RS_CONTAINER']
thread_count = 10
threads = []
queue = Queue.new
semaphore = Mutex.new
total_listed = 0
total_fetched = 0
MAX_QUEUE_PENDING = 500

puts "== Transferring files from '#{in_dir}' to '#{out_dir}' =="

# Create new Fog S3. Make sure the credentials are available from ENV.
s3 = Fog::Storage.new(:provider => 'aws', :aws_access_key_id => ENV['S3_KEY'], :aws_secret_access_key => ENV['S3_SECRET'])
rs = Fog::Storage.new(:provider => 'Rackspace', :rackspace_username => ENV['RS_USER'], :rackspace_api_key => ENV['RS_KEY'])

# Fetch the files for the bucket.
threads << Thread.new do
  Thread.current[:name] = "lister"
  puts "...started thread '#{Thread.current[:name]}'...\n"
  # Get all the files from this bucket. Fog handles pagination internally.
  s3.directories.get(in_dir).files.each do |file|
    # Add this file to the queue.
    queue.enq(file)
    total_listed += 1
    if queue.size > MAX_QUEUE_PENDING
      puts "Queue has more than #{MAX_QUEUE_PENDING} pending files, sleeping 2s"
      sleep 2
    end
  end
  # Add a final EOF message to signal the deletion threads to stop.
  thread_count.times {queue.enq(:EOF)}
end

# Delete all the files in the queue until EOF with N threads.
thread_count.times do |count|
  threads << Thread.new(count) do |number|
    target = rs.directories.get(out_dir)
    Thread.current[:name] = "move files(#{number})"
    puts "...started thread '#{Thread.current[:name]}'...\n"
    # Dequeue until EOF.
    while true
      # Dequeue the latest file and delete it. (Will block until it gets a new file.)
      file = queue.deq
      return if file == :EOF
      # Upload this file to S3. file.body?
      tf = target.files.get(file.key)
      if tf && tf.etag == file.etag
        semaphore.synchronize { puts "Skipping file #{file.key}, it exists and md5 matches" }
      else
        semaphore.synchronize { puts "Copying file #{file.key}" }
        target.files.create(:key => file.key, :body => file.body, :etag => file.etag)
      end
      # Increment the global synchronized counter.
      semaphore.synchronize {total_fetched += 1}
      puts "Moved #{total_fetched} out #{total_listed}\n" if (rand(100) == 1)
    end
  end
end

# Wait for the threads to finish.
threads.each do |t|
  begin
    t.join
  rescue RuntimeError => e
    puts "Failure on thread #{t[:name]}: #{e.message}"
  end
end
