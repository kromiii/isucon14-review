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

      chairs = db.xquery('SELECT * FROM chairs')
      chairs.each do |chair|
        chair_id = chair.fetch(:id)
        locations = db.xquery('SELECT * FROM chair_locations WHERE chair_id = ? ORDER BY created_at', chair_id)
        next if locations.count.zero?
        total_distance = 0
        locations.each_cons(2) do |(a, b)|
          total_distance += calculate_distance(
            a.fetch(:latitude),
            a.fetch(:longitude),
            b.fetch(:latitude),
            b.fetch(:longitude)
          )
        end
        latset_location = db.xquery('SELECT * FROM chair_locations WHERE chair_id = ? ORDER BY created_at DESC LIMIT 1', chair_id).first
        # 最新の位置情報をlatest_chair_locationsに保存
        db.xquery(
          'INSERT INTO latest_chair_locations (chair_id, latitude, longitude, total_distance) VALUES (?, ?, ?, ?)',
          chair_id,
          latset_location.fetch(:latitude),
          latset_location.fetch(:longitude),
          total_distance
        )
      end

      Thread.new do
        loop do
          begin
            keys = redis.keys('chair_location:*')
            values = keys.map do |key|
              data = JSON.parse(redis.get(key), symbolize_names: true)
              [key.split(':').last, data[:chair_id], data[:latitude], data[:longitude], data[:created_at]]
            end
            unless values.empty?
              placeholders = values.map { |_| "(?, ?, ?, ?, ?)" }.join(", ")
              db.xquery("INSERT INTO chair_locations (id, chair_id, latitude, longitude, created_at) VALUES #{placeholders}", *values.flatten)
              keys.each { |key| redis.del(key) }
            end
          rescue => e
            puts "Error processing Redis data: #{e.message}"
          end
        end
      end

      json(language: 'ruby')
    end
  end
end
