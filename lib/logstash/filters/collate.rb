# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"


# Collate events by time or count.
#
# The original goal of this filter was to merge the logs from different sources
# by the time of log, for example, in real-time log collection, logs can be
# collated by amount of 3000 logs or can be collated in 30 seconds.
#
# The config looks like this:
# [source,ruby]
#     filter {
#       collate {
#         count => 3000
#         interval => "30s"
#         order => "ascending"
#       }
#     }
class LogStash::Filters::Collate < LogStash::Filters::Base

  config_name "collate"

  # How many logs should be collated.
  config :count, :validate => :number, :default => 1000

  # The `interval` is the time window which how long the logs should be collated. (default `1m`)
  config :interval, :validate => :string, :default => "1m"

  # The `order` collated events should appear in.
  config :order, :validate => ["ascending", "descending"], :default => "ascending"

  public
  def register
    require "thread"
    require "rufus/scheduler"

    @mutex = Mutex.new
    @collatingDone = false
    @collatingArray = Array.new
    @scheduler = Rufus::Scheduler.new
    @job = @scheduler.every @interval do
      @logger.info("Scheduler Activated")
      @mutex.synchronize{
        collate
      }
    end
  end # def register

  public
  def filter(event)
    @logger.info("do collate filter")
    if event == LogStash::SHUTDOWN
      @job.trigger()
      @job.unschedule()
      @logger.info("collate filter thread shutdown.")
      return
    end

    # if the event is collated, a "collated" tag will be marked, so for those uncollated event, cancel them first.
    if event.get("tags").nil? || !event.get("tags").include?("collated")
      event.cancel
      @logger.info("cancelling event", :event => event)
    else
      return
    end

    @mutex.synchronize{
      @collatingArray.push(event.clone)

      @logger.info("adding to array")
      if (@collatingArray.length == @count)
        collate
      end

      if (@collatingDone)
        @collatingArray.each { |collatedEvent|
          collatedEvent.tag("collated")
          filter_matched(collatedEvent)
          @logger.info("assembling return")
          yield collatedEvent
        }
        # reset collatingDone flag
        @collatingDone = false
      end
    }
  end # def filter

  private
  def collate
    @logger.info("collating", :number => @collatingArray.length)
    if (@order == "ascending")
      # call .to_i for now until https://github.com/elasticsearch/logstash/issues/2052 is fixed
      @collatingArray.sort! { |eventA, eventB| eventA.timestamp.to_i <=> eventB.timestamp.to_i }
    else
      @collatingArray.sort! { |eventA, eventB| eventB.timestamp.to_i <=> eventA.timestamp.to_i }
    end
    @logger.info("collating done", :number => @collatingArray.length)
    @collatingDone = true
  end # def collate

  # Flush any pending messages.
  public
  def flush(options = {})
    events = []
    @logger.info("flushing", :number => @collatingArray.length, :collatingDone => @collatingDone)
    if (@collatingDone)
      @mutex.synchronize{
        while collatedEvent = @collatingArray.shift
          @logger.info("flushing event", :event => collatedEvent)
          collatedEvent.tag("collated")
          events << collatedEvent
        end # while @collatingArray.pop
      }
      # reset collatingDone flag.
      @collatingDone = false
    else
      collate
    end
    @logger.info("flushing done", :number => events.length)
    return events
  end # def flush
end #
