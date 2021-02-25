redis keys

# 汇总

- {exchange}:days 所有交易日(list)
- {exchange}:companies 所有公司 (list)
- {exchange}:company:{yyMMdd} 这一天的公司 (list)
- {exchange}:{company}:raw:1d:{yyMMdd} 原始日K
- {exchange}:{company}:adj:1d:{yyMMdd} 前复权日K
- {exchange}:{company}:raw:1h:regular:{yyMMddhh} 原始盘中小时K
- {exchange}:{company}:adj:1h:pre:{yyMMddhh} 前复权盘前小时K
- {exchange}:{company}:raw:1m:regular:{yyMMddhhmm} 原始盘中分钟K
- {exchange}:{company}:adj:1m:post:{yyMMddhhmm} 前复权盘后分钟K
- {exchange}:{company}:dividends 所有dividend (list)
- {exchange}:{company}:dividend:{yyMMdd} 单次dividend
- {exchange}:{company}:splits 所有split (list)
- {exchange}:{company}:split:{yyMMdd} 单次split
- {exchange}:{company}:ema20:1d:{yyMMdd} 每日ema20
- {exchange}:{company}:sma8:1h:{yyMMddhh} 每小时sma8
- {exchange}:{company}:options:{yyMMdd} 每日期权链 (list)
- {exchange}:{company}:option:{optionName}:raw:1d:{yyMMdd} 期权原始日K
- {exchange}:{company}:option:{optionName}:adj:1d:{yyMMdd} 期权前复权日K
- {exchange}:{company}:option:{optionName}:raw:1h:{yyMMddhh} 期权原始小时K
- {exchange}:{company}:option:{optionName}:adj:1h:{yyMMddhh} 期权前复权小时K
- {exchange}:{company}:option:{optionName}:raw:1m:{yyMMddhhmm} 期权原始分钟K
- {exchange}:{company}:option:{optionName}:adj:1m:{yyMMddhhmm} 期权前复权分钟K
- {exchange}:{company}:option:{optionName}:ema20:1d:{yyMMdd} 期权每日ema20

# list

所有list的value都是日期或这公司的字符串

# quote

所有K线数据都按下列格式 {timestamp},{open:.2f},{high:.2f},{low:.2f},{close:.2f},{volume:.2f}
