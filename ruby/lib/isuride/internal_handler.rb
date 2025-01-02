# frozen_string_literal: true

require 'isuride/base_handler'

module Isuride
  class InternalHandler < BaseHandler
    get '/matching' do
      db_transaction do |tx|
        # 未マッチのライドを待ち時間順で取得
        rides = tx.query('SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT 10')
        unless rides
          halt 204
        end

        # アクティブな椅子の最新位置情報を取得
        chairs = tx.query(<<~SQL)
          SELECT 
            chairs.id,
            COALESCE(l.latitude, 0) as latitude,
            COALESCE(l.longitude, 0) as longitude
          FROM chairs
          LEFT JOIN latest_chair_locations l ON chairs.id = l.chair_id
          WHERE chairs.is_active = TRUE
          AND NOT EXISTS (
            SELECT 1 FROM rides r
            INNER JOIN ride_statuses rs ON r.id = rs.ride_id
            WHERE r.chair_id = chairs.id
            AND rs.status != 'COMPLETED'
          )
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
