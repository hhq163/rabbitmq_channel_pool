module example

go 1.17

replace github.com/hhq163/rabbitmq_channel_pool => ../../rabbitmq_channel_pool

require github.com/streadway/amqp v1.0.0

require (
	github.com/hhq163/logger v1.0.4-0.20200925075435-2b4e66e561d3 // indirect
	github.com/natefinch/lumberjack v2.0.0+incompatible // indirect
	github.com/opentracing/opentracing-go v1.1.0 // indirect
	github.com/smallnest/chanx v1.0.0 // indirect
	go.uber.org/atomic v1.6.0 // indirect
	go.uber.org/multierr v1.5.0 // indirect
	go.uber.org/zap v1.14.1 // indirect
	gopkg.in/eapache/queue.v1 v1.1.0 // indirect
)
