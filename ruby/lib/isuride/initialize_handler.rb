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
            # chair_idごとのキーを取得
            chair_keys = redis.keys('chair_locations:*')
            
            chair_keys.each do |key|
              chair_id = key.split(':').last
              # 該当chairの全ての位置情報を取得し、JSONパース
              # rpushしているので古い順に取り出せるはず
              locations = redis.lrange(key, 0, -1).map do |item|
                data = JSON.parse(item, symbolize_names: true)
                [data[:latitude], data[:longitude], data[:created_at]]
              end
      
              unless locations.empty?                
                # latest_chair_locationsの更新
                total_distance = db.xquery('SELECT total_distance FROM latest_chair_locations WHERE chair_id = ?', chair_id).first&.fetch(:total_distance) || 0
                
                locations.each_cons(2) do |(a, b)|
                  total_distance += calculate_distance(a[0], a[1], b[0], b[1])
                end
                
                latest_location = locations.last
                existing_record = db.xquery('SELECT 1 FROM latest_chair_locations WHERE chair_id = ?', chair_id).first
                
                if existing_record
                  db.xquery(
                    'UPDATE latest_chair_locations SET latitude = ?, longitude = ?, total_distance = ?, updated_at = ? WHERE chair_id = ?',
                    latest_location[0],
                    latest_location[1],
                    total_distance,
                    latest_location[2],
                    chair_id
                  )
                else
                  db.xquery(
                    'INSERT INTO latest_chair_locations (chair_id, latitude, longitude, total_distance, updated_at) VALUES (?, ?, ?, ?, ?)',
                    chair_id,
                    latest_location[0],
                    latest_location[1],
                    total_distance,
                    latest_location[2]
                  )
                end
              end
              
              # 処理が完了したらリストを削除
              redis.del(key)
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
