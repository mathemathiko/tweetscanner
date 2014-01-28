# refs: http://morizyun.github.io/blog/ruby-twitter-stream-api-heroku/

require 'rubygems'
require 'bundler'
require 'mysql2'
require 'json'

Bundler.require

require 'twitter/json_stream'

TWITTER_CONSUMER_KEY       ||= ENV['TWITTER_CONSUMER_KEY']
TWITTER_CONSUMER_SECRET    ||= ENV['TWITTER_CONSUMER_SECRET']
TWITTER_OAUTH_TOKEN        ||= ENV['TWITTER_OAUTH_TOKEN']
TWITTER_OAUTH_TOKEN_SECRET ||= ENV['TWITTER_OAUTH_TOKEN_SECRET']
FOLLOWS                    ||= ENV['FOLLOWS']

DB_HOSTNAME  ||= ENV['DB_HOSTNAME']
DB_USER_NAME ||= ENV['DB_USER_NAME']
DB_PASSWORD  ||= ENV['DB_PASSWORD']
DB_NAME      ||= ENV['DB_NAME']

EventMachine::run do
  stream = Twitter::JSONStream.connect(
    :path => "/1.1/statuses/filter.json?follow=#{FOLLOWS}",
    :oauth => {
      :consumer_key    => TWITTER_CONSUMER_KEY,
      :consumer_secret => TWITTER_CONSUMER_SECRET,
      :access_key      => TWITTER_OAUTH_TOKEN,
      :access_secret   => TWITTER_OAUTH_TOKEN_SECRET
    },
    :ssl => true
  )

  stream.each_item do |item|
    STDOUT.print "item: #{item}\n"
    STDOUT.flush

    client = Mysql2::Client.new(
      :host     => DB_HOSTNAME,
      :username => DB_USER_NAME,
      :password => (DB_PASSWORD || ''),
      :database => DB_NAME
    )

    formatted_tweet = JSON.parse(item)

    user_id                         = client.escape(formatted_tweet['user']['id_str'])
    user_name                       = client.escape(formatted_tweet['user']['name'])
    user_screen_name                = client.escape(formatted_tweet['user']['screen_name'])
    user_image                      = client.escape(formatted_tweet['user']['profile_image_url'])
    user_description                = client.escape(formatted_tweet['user']['description']) rescue nil
    text                            = client.escape(formatted_tweet['text'])
    post_media_url                  = client.escape(formatted_tweet['entities']['media'].first['media_url']) rescue nil
    twitter_status_id               = client.escape(formatted_tweet['id_str'])
    twitter_reply_status_id         = client.escape(formatted_tweet['in_reply_to_status_id_str']) rescue nil
    twitter_reply_user_id           = client.escape(formatted_tweet['in_reply_to_user_id_str'])   rescue nil
    twitter_reply_user_screen_name  = client.escape(formatted_tweet['in_reply_to_screen_name'])   rescue nil

    client.query "INSERT INTO tweets (user_id, user_name, user_screen_name, text, post_media_url, user_image, user_description, twitter_status_id, twitter_reply_status_id, twitter_reply_user_id, twitter_reply_user_screen_name, updated_at, created_at) VALUES ('#{user_id}', '#{user_name}', '#{user_screen_name}', '#{text}', '#{post_media_url}', '#{user_image}', '#{user_description}', '#{twitter_status_id}', '#{twitter_reply_status_id}', '#{twitter_reply_user_id}', '#{twitter_reply_user_screen_name}', '#{Time.now}', '#{Time.now}')"

    client.close
  end

  stream.on_error do |message|
    STDOUT.print "error: #{message}\n"
    STDOUT.flush
  end

  stream.on_reconnect do |timeout, retries|
    STDOUT.print "reconnecting in: #{timeout} seconds\n"
    STDOUT.flush
  end

  stream.on_max_reconnects do |timeout, retries|
    STDOUT.print "Failed after #{retries} failed reconnects\n"
    STDOUT.flush
  end
end
