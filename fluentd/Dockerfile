FROM fluent/fluentd:v1.16-1

# Install FluentD plugins and required build dependencies
USER root

# Install build dependencies for native extensions
RUN apk add --no-cache \
    build-base \
    ruby-dev \
    && gem install fluent-plugin-elasticsearch \
    && gem install fluent-plugin-kafka \
    && gem install fluent-plugin-parser \
    && gem install fluent-plugin-record-reformer \
    && gem install fluent-plugin-rewrite-tag-filter \
    && apk del build-base ruby-dev

USER fluent 