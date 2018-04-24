# ActiveDataFrame

ActiveDataFrame allows efficient writing, reading, and analytical queries on large tables of numerical data. You can think of it as a persistent NumPy or NArray with good support for slicing
and aggregates without needing to load the entire dataset into memory.

The library depends on ActiveRecord and currently supports the following relational databases:
* PostgreSQL
* MySQL
* SQLite

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'active_data_frame'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install active_data_frame

## Usage

TODO: Write usage instructions here

## Development

After checking out the repo, run `bin/setup` to install dependencies. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/active_data_frame. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

