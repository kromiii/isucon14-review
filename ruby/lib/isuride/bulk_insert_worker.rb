require 'thread'

Thread.new do
  loop do
    sleep(1) # 1秒ごとにバルクインサートを実行

    items = []
    until INSERT_QUEUE.empty?
      items << INSERT_QUEUE.pop
    end

    unless items.empty?
      db_transaction do |tx|
        values = items.map { |item| "(#{item[:id]}, #{item[:chair_id]}, #{item[:latitude]}, #{item[:longitude]})" }.join(", ")
        tx.xquery("INSERT INTO chair_locations (id, chair_id, latitude, longitude) VALUES #{values}")
      end
    end
  end
end
