# frozen_string_literal: true

require 'isuride/base_handler'

module Isuride
  class InternalHandler < BaseHandler
    get '/matching' do
      db_transaction do |tx|
        # 未マッチのライドを待ち時間順で取得
        rides = tx.query('SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at')
        unless rides
          halt 204
        end

        # アクティブな椅子の最新位置情報を取得
        chairs = tx.query(<<~SQL)
          WITH chair_latest_status AS (
              SELECT
                  r.*,
                  rs.status AS ride_status,
                  ROW_NUMBER() OVER (PARTITION BY r.chair_id ORDER BY rs.created_at DESC) AS rn
              FROM rides r
              INNER JOIN ride_statuses rs ON r.id = rs.ride_id AND rs.chair_sent_at IS NOT NULL
          )
          SELECT
              c.id,
              c.is_active,
              COALESCE(l.latitude, 0) AS latitude,
              COALESCE(l.longitude, 0) AS longitude
          FROM chairs c
          LEFT JOIN chair_latest_status cls ON c.id = cls.chair_id AND cls.rn = 1
          LEFT JOIN latest_chair_locations l ON c.id = l.chair_id
          WHERE (cls.ride_status = 'COMPLETED' OR cls.ride_status IS NULL) AND c.is_active
        SQL

        # chairsを配列に変換
        chairs = chairs.map do |chair|
          {
            id: chair.fetch(:id),
            latitude: chair.fetch(:latitude),
            longitude: chair.fetch(:longitude),
          }
        end

        rides.each do |ride|
          # 最も近い椅子を見つける
          closest_chair = chairs.min_by do |chair|
            calculate_distance(
              chair[:latitude], chair[:longitude],
              ride.fetch(:pickup_latitude), ride.fetch(:pickup_longitude)
            )
          end

          if closest_chair
            tx.xquery('UPDATE rides SET chair_id = ? WHERE id = ?', 
                     closest_chair[:id], ride.fetch(:id))
            # 使用した椅子を除外
            chairs.delete(closest_chair)
          end
        end
      end

      204
    end
  end
end
