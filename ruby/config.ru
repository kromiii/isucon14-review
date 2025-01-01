# frozen_string_literal: true

$LOAD_PATH.unshift(File.join('lib', __dir__))

require 'isuride/app_handler'
require 'isuride/chair_handler'
require 'isuride/initialize_handler'
require 'isuride/internal_handler'
require 'isuride/owner_handler'
require 'isuride/base_handler'

@@worker_thread = nil

def self.start_worker
  @@worker_thread ||= Async do
    loop do
      locations = []
      while locations.size < 1000 && !Isuride::BaseHandler.class_variable_get(:@@chair_locations_queue).empty?
        locations << Isuride::BaseHandler.class_variable_get(:@@chair_locations_queue).pop(true) rescue nil
      end

      if locations.any?
        db_transaction do |tx|
          tx.xquery('INSERT INTO chair_locations (id, chair_id, latitude, longitude) VALUES (?, ?, ?, ?)', locations)
        end
      end

      sleep 1
    end
  end
end

# Initialize worker when server starts
start_worker

map '/api/app/' do
  run Isuride::AppHandler
end
map '/api/chair/' do
  use Isuride::ChairHandler
end
map '/api/owner/' do
  use Isuride::OwnerHandler
end
map '/api/internal/' do
  use Isuride::InternalHandler
end
run Isuride::InitializeHandler
