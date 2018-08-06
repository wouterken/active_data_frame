# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'active_data_frame/version'

Gem::Specification.new do |spec|
  spec.name          = "active_data_frame"
  spec.version       = ActiveDataFrame::VERSION
  spec.authors       = ["Wouter Coppieters"]
  spec.email         = ["wc@pico.net.nz"]

  spec.summary       = 'An active data frame helper'
  spec.description   = 'An active data frame helper'
  spec.homepage      = "https://github.com/wouterken/active_data_frame"

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.13"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "pry-byebug", "~> 3.4.0", '>= 3.4.0'
  spec.add_development_dependency 'pry', '~> 0.10.2', '>= 0.10.0'
  spec.add_development_dependency 'pg'
  spec.add_development_dependency 'sqlite3'
  spec.add_development_dependency 'mysql2'
  spec.add_development_dependency 'minitest', '~>5.11'
  spec.add_development_dependency 'minitest-reporters', '~> 1.1', '>= 1.1.0'
  spec.add_development_dependency 'minitest-around', '0.4.1'
  spec.add_runtime_dependency     'activerecord', '~> 5.0'
  spec.add_runtime_dependency     'rmatrix', '~> 0.1.20', '>=0.1.20'
end
