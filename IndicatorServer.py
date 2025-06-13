from flask import Flask, request, jsonify
from WindPy import w
import math
import datetime
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import logging
import json
import time

w.start()

app = Flask(__name__)


def get_formatted_date():
    # 获取当前日期时间
    now = datetime.datetime.now()
    formatted_date = now.strftime("%Y-%m-%d")
    return formatted_date

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 初始化Wind接口
def init_wind():
    try:
        w.start()
        if w.isconnected():
            logger.info("万得接口连接成功")
            return True
        else:
            logger.error("万得接口连接失败")
            return False
    except Exception as e:
        logger.error(f"万得接口初始化异常: {str(e)}")
        return False

# 获取历史价格数据
def get_historical_prices(codes, start_date, end_date, fields=None):
    """
    获取品种的历史价格数据

    参数:
    - codes: 万得代码列表，如 ['CU.SHF', 'RB.SHF']
    - start_date: 开始日期，如 '2013-01-01'
    - end_date: 结束日期，如 '2023-01-01'
    - fields: 需要查询的字段，默认为收盘价

    返回:
    - 包含历史价格的DataFrame
    """
    if not w.isconnected() and not init_wind():
        return None

    if fields is None:
        fields = "close,open,high,low,volume,amt,oi"

    result = {}
    for code in codes:
        logger.info(f"从Wind获取单只合约数据: {code} | 字段: {fields} | {start_date}~{end_date}")
        data = w.wsd(code, fields, start_date, end_date, "")
        if data.ErrorCode != 0:
            logger.error(f"合约 {code} 获取失败，ErrorCode={data.ErrorCode}")
            continue

        # 按原逻辑把 data 转成字典
        code_data = {}
        field_names = fields.split(",")
        for j, field in enumerate(field_names):
            values = []
            for idx, dt in enumerate(data.Times):
                # 某些字段可能少于 length，先 check
                if idx < len(data.Data[j]):
                    values.append((dt, data.Data[j][idx]))
            code_data[field] = values

        result[code] = code_data

    return result


# 解析交易代码
def parse_commodity_code(wind_code):
    """从Wind代码解析出商品代码和市场代码"""
    parts = wind_code.split('.')
    if len(parts) == 2:
        commodity_code = parts[0]
        market_code = parts[1]
        return commodity_code, market_code
    return None, None


# 获取商品名称
def get_commodity_names(codes):
    """获取商品代码对应的名称"""
    if not w.isconnected():
        if not init_wind():
            return {}

    try:
        data = w.wss(codes, "sec_name")
        if data.ErrorCode != 0:
            logger.error(f"获取商品名称失败: {data.ErrorCode}")
            return {}

        names = {}
        for i, code in enumerate(data.Codes):
            names[code] = data.Data[0][i] if i < len(data.Data[0]) else ""
        return names
    except Exception as e:
        logger.error(f"获取商品名称异常: {str(e)}")
        return {}


# API端点: 获取增量价格数据
@app.route('/api/incremental_prices', methods=['GET'])
def fetch_incremental_prices():
    try:
        # 获取请求参数
        codes = request.args.get('codes', '')
        if not codes:
            codes = 'pag9999,ag(t+d),ag,au(t+d),mau(t+d),au(t+n1),au(t+n2),au100g,au995,au9995,au9999,nyautn06,nyautn12,iau100g,iau995,iau999,shau,shag,pt9995,auxcnyonf,auxcny1wf,auxcny2wf,auxcny1mf,auxcny3mf,auxcny6mf,auxcny9mf,auxcny1yf,auycnyonf,auycny1wf,auycny2wf,auycny1mf,auycny3mf,auycny6mf,auycny9mf,auycny1yf,pagonf,pag1wf,pag2wf,pag1mf,pag3mf,pag6mf,pag9mf,pag1yf'
        codes_list = [code.strip() for code in codes.split(',') if code.strip()]

        last_date = request.args.get('last_date', '')
        if not last_date:
            # 默认获取最近30天的数据
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        else:
            # 从上次同步日期的后一天开始获取
            start_date = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            end_date = datetime.now().strftime('%Y-%m-%d')

        if not codes_list:
            return jsonify({"success": False, "msg": "未提供有效的代码"}), 400

        # 获取商品名称
        commodity_names = get_commodity_names(codes_list)

        # 获取数据
        price_data = get_historical_prices(codes_list, start_date, end_date)

        if price_data is None:
            return jsonify({"success": False, "msg": "获取历史价格数据失败"}), 500

        # 格式化数据，适应Java端的ShfePriceVo结构
        formatted_data = []

        for wind_code, prices in price_data.items():
            commodity_code, market_code = parse_commodity_code(wind_code)
            commodity_name = commodity_names.get(wind_code, "")

            if commodity_code and market_code:
                for date, close_price in prices.get("close", []):
                    if not pd.isna(close_price):
                        # 查找对应日期的其他价格数据
                        open_price = next((p for d, p in prices.get("open", []) if d == date), None)
                        high_price = next((p for d, p in prices.get("high", []) if d == date), None)
                        low_price = next((p for d, p in prices.get("low", []) if d == date), None)
                        volume = next((p for d, p in prices.get("volume", []) if d == date), None)
                        amount = next((p for d, p in prices.get("amt", []) if d == date), None)
                        oi = next((p for d, p in prices.get("oi", []) if d == date), None)

                        price_obj = {
                            "commodityCode": commodity_code,
                            "marketCode": market_code,
                            "symbol": wind_code,
                            "commodityName": commodity_name,
                            "tradeDate": date.strftime('%Y-%m-%d'),
                            "priceClose": float(close_price) if close_price is not None else None,
                            "priceOpen": float(open_price) if open_price is not None else None,
                            "priceHigh": float(high_price) if high_price is not None else None,
                            "priceLow": float(low_price) if low_price is not None else None,
                            "volume": float(volume) if volume is not None else None,
                            "amount": float(amount) if amount is not None else None,
                            "openInterest": float(oi) if oi is not None else None
                        }
                        formatted_data.append(price_obj)

        return jsonify({"success": True, "data": formatted_data})

    except Exception as e:
        logger.error(f"API异常: {str(e)}")
        return jsonify({"success": False, "msg": f"服务异常: {str(e)}"}), 500


# API端点: 获取历史价格数据（用于初始加载）
@app.route('/api/historical_prices', methods=['GET'])
def fetch_historical_prices():
    try:
        # 获取请求参数
        codes = request.args.get('codes', '')
        if not codes:
            codes = 'pag9999,ag(t+d),ag,au(t+d),mau(t+d),au(t+n1),au(t+n2),au100g,au995,au9995,au9999,nyautn06,nyautn12,iau100g,iau995,iau999,shau,shag,pt9995,auxcnyonf,auxcny1wf,auxcny2wf,auxcny1mf,auxcny3mf,auxcny6mf,auxcny9mf,auxcny1yf,auycnyonf,auycny1wf,auycny2wf,auycny1mf,auycny3mf,auycny6mf,auycny9mf,auycny1yf,pagonf,pag1wf,pag2wf,pag1mf,pag3mf,pag6mf,pag9mf,pag1yf'
        codes_list = [code.strip() for code in codes.split(',') if code.strip()]

        years = int(request.args.get('years', '10'))

        if not codes_list:
            return jsonify({"success": False, "msg": "未提供有效的代码"}), 400

        # 计算日期范围
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365 * years)).strftime('%Y-%m-%d')

        # 获取商品名称
        commodity_names = get_commodity_names(codes_list)

        # 为避免一次性获取数据过多，分批获取
        batch_size = 5  # 每批处理的代码数量
        batches = [codes_list[i:i + batch_size] for i in range(0, len(codes_list), batch_size)]

        all_data = []

        for batch in batches:
            # 获取数据
            batch_data = get_historical_prices(batch, start_date, end_date)

            if batch_data is None:
                logger.warning(f"获取批次数据失败: {batch}")
                continue

            # 格式化数据
            for wind_code, prices in batch_data.items():
                commodity_code, market_code = parse_commodity_code(wind_code)
                commodity_name = commodity_names.get(wind_code, "")

                if commodity_code and market_code:
                    for date, close_price in prices.get("close", []):
                        if not pd.isna(close_price):
                            # 查找对应日期的其他价格数据
                            open_price = next((p for d, p in prices.get("open", []) if d == date), None)
                            high_price = next((p for d, p in prices.get("high", []) if d == date), None)
                            low_price = next((p for d, p in prices.get("low", []) if d == date), None)
                            volume = next((p for d, p in prices.get("volume", []) if d == date), None)
                            amount = next((p for d, p in prices.get("amt", []) if d == date), None)
                            oi = next((p for d, p in prices.get("oi", []) if d == date), None)

                            price_obj = {
                                "commodityCode": commodity_code,
                                "marketCode": market_code,
                                "symbol": wind_code,
                                "commodityName": commodity_name,
                                "tradeDate": date.strftime('%Y-%m-%d'),
                                "priceClose": float(close_price) if close_price is not None else None,
                                "priceOpen": float(open_price) if open_price is not None else None,
                                "priceHigh": float(high_price) if high_price is not None else None,
                                "priceLow": float(low_price) if low_price is not None else None,
                                "volume": float(volume) if volume is not None else None,
                                "amount": float(amount) if amount is not None else None,
                                "openInterest": float(oi) if oi is not None else None
                            }
                            all_data.append(price_obj)

            # 避免Wind API请求频率过高
            time.sleep(2)

        return jsonify({"success": True, "data": all_data})

    except Exception as e:
        logger.error(f"API异常: {str(e)}")
        return jsonify({"success": False, "msg": f"服务异常: {str(e)}"}), 500


# 健康检查
@app.route('/health', methods=['GET'])
def health_check():
    connected = w.isconnected()
    if not connected:
        connected = init_wind()

    return jsonify({
        "status": "up" if connected else "down",
        "windConnected": connected,
        "timestamp": datetime.now().isoformat()
    })

# 获取SGE多种价格
@app.route('/get_sge_price', methods=['GET'])
def get_sge_prices():
    # 获取请求中的参数
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    codes = request.args.get('codes')  # 多个代码可以通过逗号分隔传递

    if not start_date:
        start_date = get_formatted_date()
    if not end_date:
        end_date = get_formatted_date()
    if not codes:
        return jsonify({"error": "Codes parameter is required"}), 400

    # 处理codes，转为WindAPI所需要的格式（多个代码）
    code_list = codes.split(",")  # 假设是通过逗号传递多个code

    result = []

    # 逐个获取每个code的数据
    for code in code_list:
        try:
            # 调用Wind API获取单个code的数据
            wsetdata = w.wsd(code, "high,low,settle,close", start_date, end_date, "TradingCalendar=SGE")

            if wsetdata.ErrorCode != 0:
                return jsonify({
                                   "error": f"Error occurred for {code}, ErrorCode: {wsetdata.ErrorCode}, Message: {wsetdata.Data}"}), 500

            # 遍历返回的每个数据项
            for i, date in enumerate(wsetdata.Times):
                high = wsetdata.Data[0][i]  # 最高价
                low = wsetdata.Data[1][i]  # 最低价
                settle = wsetdata.Data[2][i]  # 结算价
                close = wsetdata.Data[3][i]  # 收盘价

                # 格式化日期为YYYY-MM-DD
                formatted_date = date.strftime("%Y-%m-%d")

                # 将价格转为字符串，以便Java端接收BigDecimal
                high_str = None if high is None else str(high)
                low_str = None if low is None else str(low)
                settle_str = None if settle is None else str(settle)
                close_str = None if close is None else str(close)

                # 组装数据
                record = {
                    "dateTime": formatted_date,
                    "code": code,
                    "high": high_str,
                    "low": low_str,
                    "settle": settle_str,
                    "close": close_str
                }
                result.append(record)

        except Exception as e:
            return jsonify({"error": f"An error occurred for {code}: {str(e)}"}), 500

    return jsonify(result)

@app.route('/am',methods=['GET'])
def am():
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    codes = request.args.get('codes')
    if not start_date:
        start_date=get_formatted_date();
    if not end_date:
        end_date=get_formatted_date();
    if not codes:
        codes = "Y2077418"

    try:
        wsetdata = w.edb(codes,  start_date, end_date, "Fill=Previous")
        if wsetdata.ErrorCode != 0:
            return jsonify({"error": f"Error occurred, ErrorCode: {wsetdata.ErrorCode}, Message: {wsetdata.Data}"}), 500

        result = []

        for i, date in enumerate(wsetdata.Times):
            for j, code in enumerate(wsetdata.Codes):
                price = wsetdata.Data[j][i]
                # 格式化日期为YYYY-MM-DD
                formatted_date = date.strftime("%Y-%m-%d")

                # 处理NaN的价格
                if math.isnan(price):
                    price = None  # 将NaN转换为None

                # 将价格转为字符串，以便Java端接收BigDecimal
                price_str = None if price is None else str(price)

                record = {
                    "dateTime": formatted_date,
                    "code": code,
                    "price": price
                }
                result.append(record)

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500



@app.route('/pm',methods=['GET'])
def pm():
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    codes = request.args.get('codes')
    if not start_date:
        start_date=get_formatted_date();
    if not end_date:
        end_date=get_formatted_date();
    if not codes:
        codes = "T7305994"

    try:
        wsetdata = w.edb(codes,  start_date, end_date, "Fill=Previous")
        if wsetdata.ErrorCode != 0:
            return jsonify({"error": f"Error occurred, ErrorCode: {wsetdata.ErrorCode}, Message: {wsetdata.Data}"}), 500

        result = []

        for i, date in enumerate(wsetdata.Times):
            for j, code in enumerate(wsetdata.Codes):
                price = wsetdata.Data[j][i]
                formatted_date = date.strftime("%Y-%m-%d")

                if math.isnan(price):
                    price = None

                price_str = None if price is None else str(price)

                record = {
                    "dateTime": formatted_date,
                    "code": code,
                    "price": price
                }
                result.append(record)

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

@app.route('/deferredFee', methods=['GET'])
def deferred_fee():
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')

    if not start_date or not end_date:
        return jsonify({"error": "Please provide both startDate and endDate"}), 400

    # 获取数据
    try:
        wsetdata = w.edb("S0182163,S0270855,S0270857,S0206703,S0182164", start_date, end_date, "Fill=Previous")

        if wsetdata.ErrorCode != 0:
            return jsonify({"error": f"Error occurred, ErrorCode: {wsetdata.ErrorCode}, Message: {wsetdata.Data}"}), 500

        result = []

        for i, date in enumerate(wsetdata.Times):
            for j, code in enumerate(wsetdata.Codes):
                price = wsetdata.Data[j][i]
                # 格式化日期为YYYY-MM-DD
                formatted_date = date.strftime("%Y-%m-%d")

                # 处理NaN的价格
                if math.isnan(price):
                    price = None  # 将NaN转换为None

                # 将价格转为字符串，以便Java端接收BigDecimal
                price_str = None if price is None else str(price)

                record = {
                    "dateTime": formatted_date,
                    "code": code,
                    "price": price
                }
                result.append(record)

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=False)
