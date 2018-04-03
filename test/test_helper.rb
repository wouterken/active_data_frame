$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'pry'
require 'pry-byebug'
require 'active_data_frame'
require 'minitest/autorun'
require 'minitest/reporters'
require 'setup/seeds'
require 'setup/transactional_test'

Minitest::Reporters.use! [Minitest::Reporters::SpecReporter.new()]