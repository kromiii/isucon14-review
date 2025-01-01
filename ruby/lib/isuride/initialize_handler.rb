# frozen_string_literal: true

require 'open3'

require 'isuride/base_handler'

module Isuride
  class InitializeHandler < BaseHandler
    PostInitializeRequest = Data.define(:payment_server)

    post '/api/initialize' do
      req = bind_json(PostInitializeRequest)

      out, status = Open3.capture2e('../sql/init.sh')
      unless status.success?
        raise HttpError.new(500, "failed to initialize: #{out}")
      end

      db.xquery("UPDATE settings SET value = ? WHERE name = 'payment_gateway_url'", req.payment_server)

      Thread.new do
        loop do
          begin
            keys = redis.keys('chair_location:*')
            values = keys.map do |key|
              data = JSON.parse(redis.get(key), symbolize_names: true)
              [key.split(':').last, data[:chair_id], data[:latitude], data[:longitude]]
            end
            unless values.empty?
              placeholders = values.map { |_| "(?, ?, ?, ?)" }.join(", ")
              db.xquery("INSERT INTO chair_locations (id, chair_id, latitude, longitude) VALUES #{placeholders}", *values.flatten)
              keys.each { |key| redis.del(key) }
            end
          rescue => e
            puts "Error processing Redis data: #{e.message}"
          end
          sleep 1
        end
      end

      json(language: 'ruby')
    end
  end
end
