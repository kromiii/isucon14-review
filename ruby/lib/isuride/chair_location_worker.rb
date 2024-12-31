# frozen_string_literal: true

module Isuride
  class ChairLocationWorker
    BATCH_SIZE = 1000
    PROCESS_INTERVAL = 0.1

    def initialize
      @queue = Queue.new
      @running = true
      start_worker
    end

    def enqueue(location_data)
      @queue << location_data
    end

    def stop
      @running = false
    end

    private

    def start_worker
      @thread = Thread.new do
        while @running
          process_batch
          sleep PROCESS_INTERVAL
        end
      end
    end

    def process_batch
      batch = []
      while batch.size < BATCH_SIZE && !@queue.empty?
        batch << @queue.pop
      end

      return if batch.empty?

      values = batch.map { |data| 
        "(#{data[:id]}, #{data[:chair_id]}, #{data[:latitude]}, #{data[:longitude]})"
      }.join(", ")
      
      db_transaction do |tx|
        tx.xquery(
          "INSERT INTO chair_locations (id, chair_id, latitude, longitude) VALUES #{values}"
        )
      end
    end
  end
end
