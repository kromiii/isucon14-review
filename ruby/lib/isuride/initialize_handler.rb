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

      # chair_locationsからlatest_chair_locationsにデータを移行
      # 既存の chair_locations を chair_id ごとにまとめ、create_atで昇順に並べる
      # chair_idごとに総走行距離を計算し、total_distanceに保存
      # その後、最新のlatitude, longitudeをlatest_chair_locationsに保存
      # total_distanceとlatest_chair_locationsからlatest_chair_locationsを作成
      chairs = db.xquery('SELECT * FROM chairs')
      chairs.each do |chair|
        chair_id = chair.fetch(:id)
        locations = db.xquery('SELECT * FROM chair_locations WHERE chair_id = ? ORDER BY created_at', chair_id)
        next if locations.empty?
        total_distance = 0
        locations.each_cons(2) do |(a, b)|
          total_distance += calculate_distance(
            a.fetch(:latitude),
            a.fetch(:longitude),
            b.fetch(:latitude),
            b.fetch(:longitude)
          )
        end
        # 最新の位置情報をlatest_chair_locationsに保存
        # db.xquery('INSERT INTO latest_chair_locations (chair_id, latitude, longitude, total_distance) VALUES (?, ?, ?, ?)', chair_id, locations.last[:latitude], locations.last[:longitude], total_distance)
      end

      # chair_locationsのデータを非同期でDBに保存するためのスレッド
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
          sleep 0.1
        end
      end

      json(language: 'ruby')
    end
  end
end
