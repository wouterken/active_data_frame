# ActiveDataFrame

ActiveDataFrame allows efficient writing, reading, and analytical queries on large tables of numerical data. You can think of it as a persistent NumPy or NArray with good support for slicing and aggregates without needing the entire dataset in memory.

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

## Examples

### Using the generator

```bash
# Generate a new data frame named Statistic, with a datapoint type of double, and a block size of 100
rails generate active_data_frame:install Statistic double 100

# Then run migrations to create the underlying table
rake db:migrate
````

## Usage
### Generator
The easiest way to get started is to use the in-built generator to generate a new
`ActiveDataFrame`. This will generate the required migrations for the data frame
and generate a new module that you can include inside an `ActiveRecord` model to give it access to the frame.

```bash
# Generate a new MeterReading data frame type, with a block type of
# double and a block size of 48 data points

rails generate active_data_frame:install MeterReading double 48

# Generate a new Dimension data frame type, with a block type of
# float and a block size of 10 data points.
# Inject the data-type for use into the Iris model

rails generate active_data_frame:install Dimension float 10 Iris

#
# Generate a new status data frame type with an integer block type
#
rails generate active_data_frame:install Status integer
```

### Writing to a data frame
When you include a data frame in an ActiveRecord model, each instance of the model corresponds to a single row in the data frame. The columns are a series of points that stretch towards infinity in each direction.

By default columns are indexed by integers, but you can set a static or dynamic column map so that you can easily have columns indexed by time, enum columns or use any other data type that serves as a useful index.

You can write any number of data points to a row in the dataframe using #[]=

```ruby
#E.g.
# Write to the row called readings from index 0. Here Sensor is the ActiveRecord model, readings is the name of the row
Sensor.first.readings[0] = 1,2,3

# Write to the row called readings from an offset at 1_000_000
Sensor.first.readings[1_000_000] = -10, -9, -8

#Writing to a row which has a column mapping applied, mapping times on integer indexes
MeterChannel.first.readings['2001-01-01'] = [1.3, 3.4]

#If you have enum columns you can use the #[enum_name]= setter instead.
Iris.first.dimensions.sepal_length = 5.3
Iris.first.dimensions.petal_width  = 4.3

# You can set data for multiple rows at once, by using the frame accessor on the model's class instead of an instance.

E.g.
# This sets the reading at index 1 to 5 for ALL sensors
Sensor.readings[1] = 5

# You can use AR queries to refine which set of rows you are updating at once.
# E.g.
MeterChannel.where("created_at < ?", "2001-01-01").readings['2001-01-01'] = [5,6,7]
```

ActiveDataFrame supports very quick writing of 1000's of values for a single row at a time. Don't be afraid to write large arrays of data like this.

### Reading from a data frame
Reading from a data frame is similar to writing and uses the #[] method.
You can read individual values, a range of values, and sparse selections of columns.

```ruby
#E.g.
# Read a single value
Sensor.first.readings[0] # => Matrix(1x1)[...]

# Read a range of 3 values values
Sensor.first.readings[0...3] # => Matrix(1x3)[...]

# Read some non contiguous values and ranges
Sensor.first.readings[5, 10, 4..7, 9..10] = Matrix(1x8)[...]

#Reading from a row which has a column mapping that uses times
MeterChannel.first.readings['2001-01-01'...'2002-01-01'] = Matrix(1xM)[....]

#If you have enum columns you can use the #[enum_name] getter for single columns
Iris.first.dimensions.sepal_length
Iris.first.dimensions.petal_width

# And use symbols as column indices (this assumes a specific ordering of enum columns)
Iris.first.dimensions[:sepal_length...:petal_width]
```

Similar to when writing data, you can also read data from multiple rows at once.
Just use the active data frame accessor on the model class instead of a model instance. E.g.

```ruby
Sensor.readings[0..5] # => Matrix(Nx5)
```

### Deleting
    You can use #clear(range_or_indices) to delete data.


    Deleting data is equivalent to setting all data points to zero.
    So the operation row[index] = [0, 0, 0, 0.....0] is equivalent
    to the operation row.clear(index...end_index). ActiveDataFrame
    will automatically trim empty blocks.

### Batching
If performing many small reads and writes from a data frame in a single atomic operation
it makes sense to do this in a single transaction. Active Data Frame provides the `ActiveDataFrame::Database.batch do ... end` method. This method will not only ensure your operations occur in a single transaction, but also that they are sent to the underlying database adapter as a single command.

### Analytical Queries
Any read of a dataframe returns an RMatrix instance. An RMatrix supports a large number of
statistical methods and list methods. (See the RMatrix readme for more details).
E.g.

```ruby
cpu_loads = CPU.first.loads['2001-01-01'..'2005-01-01']
puts cpu_loads.avg
puts cpu_loads.stddev
puts cpu_loads.max
# ... and many more
```

However in some cases you are dealing with so much data it is not possible, or too slow to retreive all the data at once and manipulate in-memory. ActiveDataFrame supports performing a number of aggregate methods directly in the database. These are #avg, #min, #max and #sum. The syntax for this is almost identical to an ordinary read.

```ruby
CPU.loads.avg['2001-01-01'...'2005-01-01'] # The average CPU load per period over all CPUS

CPU.where(manufacturer: :intel).loads.min['2001-01-01'...'2005-01-01'] # The minimum CPU load per period over all intel CPUS
```

### Categorical data
ActiveDataFrame provides a very basic abstraction for storing categorical data. This is done by storing categories as an integer data frame, and providing a map from integers to categories. The library will then allow you to use the category names in place of the raw underlying integers.
E.g.

```ruby
module HasStatus
  include ActiveDataFrame::HasDataFrame('status', Blocks::StatusBlock, value_map: {
    actual: 2,
    estimated: 1,
    unknown: 0
  })
end

class CPU < ApplicationRecord
  include HasStatus
end
```

The CPU model above includes a dataframe with a status mapping. We can now do things like

```ruby
CPU.first.status[0]    # => :unknown
CPU.first.status[0..5] # => [:unknown,:unknown,:unknown,:unknown,:unknown]

CPU.first.status[0] = :actual, :estimated
CPU.first.status[0..5] # => [:actual,:estimated,:unknown,:unknown,:unknown]
```

### Time-series data
We can use any datatype we like to index into a dataframe, so long as we can map it to an integer index. This makes active dataframes very well suited to storing large streams of interval data over time.

For example we might define a mapping such that every half hour period in time corresponds to a colum in our dataframe. In the below example we might be counting the number of arrivals at an airport every half-hour.

```ruby
module HasArrivals
  include ActiveDataFrame::HasDataFrame('arrivals', Blocks::ArrivalBlock)
  module ColumnMaps
    def self.included(base)
      base.arrivals_column_map Hash.new{|hash, time| ((time.to_time - Time.at(0)) / 1.hour).to_i rescue time.to_i }
    end
  end
end

class Airport < ApplicationRecord
  include HasArrivals::ColumnMaps, HasArrivals
end
```

Now we can use any value that implements #to_time to index into our dataframe. This supports both single indexes and ranges (...).
E.g.

```ruby
Airport.first.arrivals['2001-01-01'...'2002-01-01'] = Matrix(1xM)[....]
```

### Column Mappings
We can use any datatype we like to index into a dataframe, so long as we can map it to an integer index. See the section on Time-series data for one example of this. Columns can also be aliases to categories. An example of this is using ActiveDataFrame to model the classic Iris dataset.

```ruby
class Iris < ApplicationRecord
  include HasDimensions
  dimension_column_names %i(sepal_length sepal_width petal_length petal_width)
end
```

Here we have mapped the first four columns of our data frame to sepal_length, sepal_width, petal_length and petal_width.

When using symbols as column names ActiveDataFrame provides some syntactic sugar for easily slicing and dicing frames.

We can do things like:

* Extract a slice of data:

    `iris_results = Iris.where(species: :setosa).dimension[:sepal_width..:petal_length]`
* Extract an entire column from a data-set using the column name:

    `iris_results.sepal_width => V[[...]`]
* Extract an entire column from a data-set using the column name:

    `iris_results.sepal_width => V[[...]`]
* Extract a single value from an instance:

    `Iris.first.dimension.sepal_width.to_f`

* Set one or more values for an instance or row at once:

    `Iris.first.dimension.sepal_width = 13`
    `Iris.all.dimension.petal_length = 5.2,6.3,5.4,1.1`

### Configuration
ActiveDataFrame supports project-wide configuration using

```ruby
ActiveDataFrame.config do |config|
  config.[config_option_name] = [config_value]
end
```

Currently the following configuration options are supported:

* `suppress_logs` The queries generated by ActiveDataFrame are quite verbose. If you would like to supress ActiveRecord logging for these queries, set this option to `true`
## Development

After checking out the repo, run `bin/setup` to install dependencies. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Testing

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/active_data_frame. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

