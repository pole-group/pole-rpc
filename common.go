// @Author:  lesss
// @Description: 
// @Date:  2021/5/30 23:33 

package pole_rpc

import (
	"encoding/json"
	"runtime"
)

//ToJson JSON 格式化打印
func ToJson(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

//GetPanicStack 获取堆栈信息
func GetPanicStack() string {
	var buf [4096]byte
	n := runtime.Stack(buf[:], false)
	return string(buf[:n])
}
