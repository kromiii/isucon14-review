# frozen_string_literal: true

require 'isuride/base_handler'

module Isuride
  class InternalHandler < BaseHandler
    get '/matching' do
      db.transaction do
        # 未マッチのライドを待ち時間順で取得
        rides = db.query('SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT 10')
        return 204 if rides.empty?

        # アクティブな椅子の最新位置情報を取得
        chairs = db.query(<<~SQL)
          SELECT 
            chairs.*, 
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

        rides.each do |ride|
          next if chairs.empty?
          
          # 最も近い椅子を見つける
          closest_chair = chairs.min_by do |chair|
            calculate_distance(
              chair.fetch(:latitude), chair.fetch(:longitude),
              ride.fetch(:pickup_latitude), ride.fetch(:pickup_longitude)
            )
          end

          if closest_chair
            db.xquery('UPDATE rides SET chair_id = ? WHERE id = ?', 
                     closest_chair.fetch(:id), ride.fetch(:id))
            # 使用した椅子を除外
            chairs.delete(closest_chair)
          end
        end
      end

      204
    end
  end
end
