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
          'INSERT INTO latest_chair_locations (chair_id, latitude, longitude, total_distance, updated_at) VALUES (?, ?, ?, ?, ?)',
          chair_id,
          latset_location.fetch(:latitude),
          latset_location.fetch(:longitude),
          total_distance,
          latset_location.fetch(:created_at)
        )
      end

      Thread.new do
        loop do
          begin
            # バルク更新用の配列を準備
            upserts = []
            chair_keys = redis.keys('chair_locations:*')

            chair_keys.each do |key|
              chair_id = key.split(':').last
              locations = redis.lrange(key, 0, -1).map do |item|
                data = JSON.parse(item, symbolize_names: true)
                [data[:latitude], data[:longitude], data[:created_at]]
              end

              unless locations.empty?
                latest_chair_location = db.xquery('SELECT * FROM latest_chair_locations WHERE chair_id = ?', chair_id).first
                total_distance = latest_chair_location ? latest_chair_location.fetch(:total_distance) : 0

                unless latest_chair_location.nil?
                  locations.unshift([latest_chair_location.fetch(:latitude), latest_chair_location.fetch(:longitude), latest_chair_location.fetch(:updated_at)])
                end
                
                locations.each_cons(2) do |(a, b)|
                  total_distance += calculate_distance(a[0], a[1], b[0], b[1])
                end

                latest_location = locations.last
                upserts << [chair_id, latest_location[0], latest_location[1], total_distance, latest_location[2]]
              end
              
              redis.del(key)
            end

            # 一括upsertの実行
            unless upserts.empty?
              db.xquery(
                'INSERT INTO latest_chair_locations (chair_id, latitude, longitude, total_distance, updated_at) VALUES ' +
                upserts.map { '(?, ?, ?, ?, ?)' }.join(',') +
                ' ON DUPLICATE KEY UPDATE latitude=VALUES(latitude), longitude=VALUES(longitude), total_distance=VALUES(total_distance), updated_at=VALUES(updated_at)',
                *upserts.flatten
              )
            end
          rescue => e
            puts "Error processing Redis data: #{e.message}"
          end

          sleep 0.1
        end
      end

      json(language: 'ruby')
    end
  end
end
