package base

import(
	"time"
	br "github.com/hhq163/breaker"
)
var Breakers br.Breakers

func BreakerInit(){
	options := br.Options{
		BucketTime:        150 * time.Millisecond,
		BucketNums:        6450, //每秒4万次请求，超过这个值熔断
		BreakerRate:       0.3,  //错误率阀值
		BreakerMinSamples: 300,
		CoolingTimeout:    3 * time.Second,        //冷却时间，打开后，过冷却时间后变成半打开
		DetectTimeout:     150 * time.Millisecond, //检测时间，半打开状态以检测时间去发送请求，成功次数到达HalfOpenSuccess后，关闭熔断器
		HalfOpenSuccess:   3,
	}
	Breakers = br.InitBreakers([]int32{1000}, options)
}

func GetBreaker(cmd int32)*br.Breaker{
	return Breakers.GetBreaker(cmd)
}