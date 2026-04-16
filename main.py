from pathlib import Path
import mimetypes
import clickhouse_connect
from litestar import Litestar, get
from litestar.static_files.config import StaticFilesConfig
from litestar.response import Response
from datetime import datetime, time, timedelta, date 
import holidays
import pandas as pd
import asyncio
from collections import Counter

# 明确添加 .js 文件的 MIME 类型，确保浏览器能正确识别脚本
mimetypes.add_type("application/javascript", ".js")

_client = None
async def get_client():
    """获取ClickHouse数据库连接客户端"""
    global _client
    if _client is None:
        _client = await clickhouse_connect.get_async_client(
            host="10.24.5.59",
            port=8123,
            username="cheakf",
            password="Swq8855830.",
            database="dwd"
        )
    return _client


status_map = {
    "已超时": "bg-primary-container text-white",
    "执行中": "bg-[#00C853] text-white",
    "待开工": "bg-surface-container-highest text-on-surface",
    "已完成": "bg-[#00C853] text-white"
}


def format_time(dt):
    """
    格式化时间显示
    - 将datetime/time对象格式化为 'MM-DD HH:MM' 格式
    - 处理1970年1月1日的无效日期（返回空字符串）
    - 处理空值情况（返回 '--:--'）
    """
    if dt is None:
        return "--:--"
    if isinstance(dt, datetime):
        # 1970年1月1日是ClickHouse中的默认空值，返回空字符串
        if dt.year == 1970 and dt.month == 1 and dt.day == 1:
            return ""
        return dt.strftime('%m-%d %H:%M')
    elif isinstance(dt, time):
        return dt.strftime('%H:%M')
    try:
        # 尝试解析ISO格式字符串
        dt_iso = datetime.fromisoformat(str(dt))
        if dt_iso.year == 1970 and dt_iso.month == 1 and dt_iso.day == 1:
            return ""
        return dt_iso.strftime('%m-%d %H:%M')
    except (ValueError, TypeError):
        return "--:--"

def is_valid_time(dt):
    """
    检查时间是否有效（非空且非1970年1月1日）
    用于判断实际开工/完工时间是否存在
    """
    if dt is None:
        return False
    if isinstance(dt, datetime):
        return not (dt.year == 1970 and dt.month == 1 and dt.day == 1)
    return False

def get_workdays_in_last_n_days(n=7):
    """
    获取近N个工作日日期（排除法定节假日和周末）
    返回包含N个工作日的列表
    """
    cn_holidays = holidays.CN(years=range(2020, 2030))
    workdays = []
    current_date = date.today()
    
    while len(workdays) < n:
        # 检查是否是节假日或周末（周六=5，周日=6）
        if current_date not in cn_holidays and current_date.weekday() < 5:
            workdays.append(current_date)
        current_date -= timedelta(days=1)
    
    workdays.reverse()
    return workdays



@get("/api/table-data")
async def get_table_data(team: str | None = None) -> dict:
    """
    获取表格数据的主API接口
    参数:
        - team: 班组名称过滤（控制整个页面的所有数据）
    返回:
        - table_data: 今日执行队列数据
        - summary: 汇总统计数据
    """
    try:
        # 获取ClickHouse数据库连接客户端
        client = await get_client()
        # 过滤用sql
        team_filter = f"AND BILL.`班组名称` = '{team}'" if team else ""
        # ===================== 3.1 主查询 =====================
        # 查询当前月及近7天的生产计划数据
        # 包含字段：项目号、车号、节车号、排程时间、计划时间、实际时间、班组、是否兑现节拍、是否准时开完工
        query = f"""
            SELECT 
                today() AS ch_today,  -- 获取数据库当前日期
                BILL.`项目号` , 
                BILL.`车号` , 
                BILL.`节车号` , 
                BILL.`工序编码`,
                BILL.`工序名称`,
                BILL.`排程开始时间` , 
                BILL.`排程结束时间` , 
                BILL.`计划开始时间` , 
                BILL.`计划结束时间` , 
                BILL.`实际开始时间` , 
                BILL.`实际结束时间` , 
                BILL.`班组名称` , 
                BILL.`是否兑现节拍` , 
                BILL.`是否准时开完工` 
            FROM 
                dwd.beat_fulfillment_rate BILL 
            WHERE 
                (toStartOfMonth(toDate(BILL.`计划开始时间`)) = toStartOfMonth(today()  -- 本月数据
                OR toDate(BILL.`实际结束时间`) = today()  -- 今日完工数据
                OR toDate(BILL.`计划开始时间`) >= today() - INTERVAL 6 DAY AND toDate(BILL.`计划开始时间`) <= today())  -- 近7天计划
                {team_filter})
            ORDER BY 
                BILL.`计划开始时间` DESC
        """
        # ===================== 3.2 节拍兑现率趋势查询 =====================
        # 查询近7天的节拍兑现率，按日期分组
        trend_query = f"""
            SELECT 
                toDate(BILL.`计划开始时间`) AS plan_date,  -- 计划开工日期
                COUNT(*) AS total,  -- 总数
                sum(if(BILL.`是否兑现节拍` = '是', 1, 0)) AS beat_ok  -- 节拍达标数
            FROM 
                dwd.beat_fulfillment_rate BILL 
            WHERE 
                toDate(BILL.`计划开始时间`) >= today() - INTERVAL 6 DAY
                AND toDate(BILL.`计划开始时间`) <= today()
                {team_filter}
            GROUP BY toDate(BILL.`计划开始时间`)
            ORDER BY plan_date
        """
        # ===================== 3.3 准时开完工率趋势查询 =====================
        # 查询近7天的准时开完工率，按日期分组
        ontime_trend_query = f"""
            SELECT 
                toDate(BILL.`计划开始时间`) AS plan_date,  -- 计划开工日期
                COUNT(*) AS total,  -- 总数
                sum(if(BILL.`是否准时开完工` = '是', 1, 0)) AS on_time_ok  -- 准时数
            FROM 
                dwd.beat_fulfillment_rate BILL 
            WHERE 
                toDate(BILL.`计划开始时间`) >= today() - INTERVAL 6 DAY
                AND toDate(BILL.`计划开始时间`) <= today()
                {team_filter}
            GROUP BY toDate(BILL.`计划开始时间`)
            ORDER BY plan_date
        """
        
        # 并发查询
        task_list = [
            client.query_df(query),
            client.query_df(trend_query),
            client.query_df(ontime_trend_query),
        ]
        (
            result,
            trend_result,
            ontime_trend_result,
        ) = await asyncio.gather(*task_list, return_exceptions=False)
        
        # ===================== 3.4 初始化统计变量 =====================
        table_data = []  # 表格数据列表
        status_counts = Counter()  # 状态计数（待开工、执行中、已超时）
        now = datetime.now()  # 当前时间，用于判断是否超时
        
        # 今日概况统计
        today_scheduled = 0  # 今日应完工序数量（计划开工时间为今日）
        today_completed = 0  # 今日已完成数量（实际结束时间为今日）
        today_remaining = 0  # 今日剩余数量
        
        # 今日指标统计
        today_beat_ok = 0  # 今日节拍达标数
        today_on_time_ok = 0  # 今日准时数
        
        # 本月指标统计
        month_total = 0  # 本月总数量
        month_beat_ok = 0  # 本月节拍达标数
        month_on_time_ok = 0  # 本月准时数

        # 近7日趋势数据初始化
        last_7_days_beat = []  # 节拍率趋势
        last_7_days_on_time = []  # 准时率趋势
        
        # 初始化近7日字典，按日期聚合
        days_dict = {}
        for i in range(7):
            d = (date.today() - timedelta(days=6-i))
            days_dict[d] = {"total": 0, "beat_ok": 0, "on_time_ok": 0}
            
        for index, row in result.iterrows():
            status = ""
            if not is_valid_time(row["实际开始时间"]) and not is_valid_time(row["实际结束时间"]):
                status = "待开工"
            elif is_valid_time(row["实际开始时间"]) and not is_valid_time(row["实际结束时间"]):
                if row["计划结束时间"] > 
                status = "执行中"
            elif is_valid_time(row["实际开始时间"]) and is_valid_time(row["实际结束时间"]):
                status = "已完成"    
            
            
            
            table_data.append({
                "status": status,
                "status_class": status_map.get(status, ""),
                "project": row['项目号'],
                "train_no": row['车号'],
                "car_no": row['节车号'],
                "process_code": row['工序编码'],
                "process_name": row['工序名称'],
                "plan_start": format_time(plan_start),
                "plan_end": format_time(plan_end),
                "actual_start": format_time(actual_start),
                "actual_end": format_time(actual_end),
                "scheduled_duration": scheduled_duration,
                "execution_duration": execution_duration,
                "is_overtime": is_overtime,
                "is_pending": is_pending
            })
        

        for row in result.named_results():
            # ===================== 3.5 处理每条记录 =====================
            
            # 统一处理时间字段，移除时区信息（避免时区差异问题）
            plan_start = row['计划开始时间'].replace(tzinfo=None) if row['计划开始时间'] else None
            plan_end = row['计划结束时间'].replace(tzinfo=None) if row['计划结束时间'] else None
            actual_start = row['实际开始时间'].replace(tzinfo=None) if row['实际开始时间'] else None
            actual_end = row['实际结束时间'].replace(tzinfo=None) if row['实际结束时间'] else None
            
            # 适配新的字段逻辑：判断是否完工
            is_finished = actual_end is not None
            dispatch_status = "完工" if is_finished else ("开工" if actual_start else "待开工")

            # ===================== 计算状态 =====================
            # 状态分为三种：待开工、执行中、已超时
            status = "待开工"  # 默认状态
            is_pending = True  # 是否待开工
            is_overtime = False  # 是否超时
            
            # 检查实际开工时间是否有效（非空且非1970年）
            has_actual_start = is_valid_time(actual_start)
            # 检查实际完工时间是否有效（非空且非1970年）
            has_actual_end = is_valid_time(actual_end)
            
            if has_actual_start and has_actual_end:
                # 已完工: 实际开工时间和实际结束时间都存在
                status = "已完成"
                is_pending = False
            elif has_actual_start and not has_actual_end:
                # 执行中: 实际开工时间存在且实际结束时间不存在
                status = "执行中"
                is_pending = False
                if plan_end and now > plan_end:
                    # 已超时: 当前时间 > 计划结束时间
                    status = "已超时"
                    is_overtime = True
            
            # ===================== 统计本月指标 =====================
            # 严格按照用户要求：
            # - 分母：本月实际结束时间不为空且不为1970-01-01的数据数量
            # - 分子：实际时长 <= 排程时长的数据数量
            if plan_start and plan_start.year == local_today.year and plan_start.month == local_today.month:
                if is_valid_time(actual_end):
                    month_total += 1
                    scheduled_duration_val = (plan_end - plan_start).total_seconds() / 60 if plan_start and plan_end else None
                    actual_duration_val = (actual_end - actual_start).total_seconds() / 60 if actual_start else None
                    if scheduled_duration_val is not None and actual_duration_val is not None and actual_duration_val <= scheduled_duration_val:
                        month_beat_ok += 1
                if row['是否准时开完工'] == '是':
                    month_on_time_ok += 1

            # ===================== 统计今日概况 =====================
            # 应完工序数量：统计计划开工时间为今日的数据数量
            if plan_start and plan_start.date() == local_today:
                today_scheduled += 1
                if is_valid_time(actual_end):
                    scheduled_duration_val = (plan_end - plan_start).total_seconds() / 60 if plan_start and plan_end else None
                    actual_duration_val = (actual_end - actual_start).total_seconds() / 60 if actual_start else None
                    if scheduled_duration_val is not None and actual_duration_val is not None and actual_duration_val <= scheduled_duration_val:
                        today_beat_ok += 1
                if row['是否准时开完工'] == '是':
                    today_on_time_ok += 1
            # 已完成数量：统计实际结束时间为今日的数据数量
            if actual_end and actual_end.date() == local_today:
                today_completed += 1

            # ===================== 近7日趋势统计 =====================
            # 按计划开工日期聚合统计
            if plan_start:
                p_date = plan_start.date()
                if p_date in days_dict:
                    days_dict[p_date]["total"] += 1
                    if row['是否兑现节拍'] == '是':
                        days_dict[p_date]["beat_ok"] += 1
                    if row['是否准时开完工'] == '是':
                        days_dict[p_date]["on_time_ok"] += 1

            # ===================== 仅展示当日数据到执行队列 =====================
            # 只展示计划开始时间为当日的数据
            if plan_start and plan_start.date() == local_today:
                status_counts[status] += 1
                
                # ===================== 计算时长 =====================
                # 计划时长：计划结束时间 - 计划开始时间
                scheduled_duration = "--"
                if plan_start and plan_end:
                    scheduled_duration = f"{int((plan_end - plan_start).total_seconds() / 60)}M"

                # 执行时长：实际结束时间 - 实际开始时间（两者都有效且非1970-01-01）
                execution_duration = "--"
                if is_valid_time(actual_start) and is_valid_time(actual_end):
                    execution_duration = f"{int((actual_end - actual_start).total_seconds() / 60)}M"

                # 只展示待开工、执行中、已超时状态的数据，不展示已完成
                if status == "已完成":
                    pass
                else:
                    table_data.append({
                        "status": status,
                        "status_class": status_map.get(status, ""),
                        "project": row['项目号'],
                        "train_no": row['车号'],
                        "car_no": row['节车号'],
                        "process_code": row['工序编码'],
                        "process_name": row['工序名称'],
                        "plan_start": format_time(plan_start),
                        "plan_end": format_time(plan_end),
                        "actual_start": format_time(actual_start),
                        "actual_end": format_time(actual_end),
                        "scheduled_duration": scheduled_duration,
                        "execution_duration": execution_duration,
                        "is_overtime": is_overtime,
                        "is_pending": is_pending
                    })
        
        # ===================== 3.6 计算百分比指标 =====================
        # 本月节拍兑现率 = 本月节拍达标数 / 本月总数 * 100
        month_beat_rate = round((month_beat_ok / month_total * 100), 1) if month_total > 0 else 0
        # 本月准时开完工率 = 本月准时数 / 本月总数 * 100
        month_on_time_rate = round((month_on_time_ok / month_total * 100), 1) if month_total > 0 else 0
        
        # 今日节拍兑现率
        today_beat_rate = round((today_beat_ok / today_scheduled * 100), 1) if today_scheduled > 0 else 0
        # 今日准时开完工率
        today_on_time_rate = round((today_on_time_ok / today_scheduled * 100), 1) if today_scheduled > 0 else 0
        
        # 计算剩余量 (严格按照用户要求: 应完工序数量 - 已完成数量)
        # 增加 max(0) 保护，防止负数显示
        today_remaining = max(0, today_scheduled - today_completed)
        
        # ===================== 3.7 计算近7日趋势数据 =====================
        # 使用 trend_result 独立查询的数据来计算节拍率趋势
        
        # 初始化近7日趋势字典（仅工作日）
        trend_days_dict = {}
        workdays = get_workdays_in_last_n_days(7)
        for d in workdays:
            trend_days_dict[d] = {"total": 0, "beat_ok": 0, "on_time_ok": 0}
        
        # 填充节拍率趋势数据
        for trend_row in trend_result.named_results():
            p_date = trend_row['plan_date']
            if p_date in trend_days_dict:
                trend_days_dict[p_date]["total"] = trend_row['total']
                trend_days_dict[p_date]["beat_ok"] = trend_row['beat_ok']
        
        # 转换为百分比列表格式
        for d in sorted(trend_days_dict.keys()):
            day_data = trend_days_dict[d]
            beat_rate = round((day_data["beat_ok"] / day_data["total"] * 100), 1) if day_data["total"] > 0 else 0
            last_7_days_beat.append({"date": d.strftime("%m-%d"), "rate": beat_rate})
        
        # ===================== 计算准时率趋势 =====================
        # 使用 team 参数控制准时率趋势数据
        if team:
            # 使用独立查询的准时率数据
            ontime_days_dict = {}
            for d in workdays:
                ontime_days_dict[d] = {"total": 0, "on_time_ok": 0}
            
            for ontime_row in ontime_trend_result.named_results():
                p_date = ontime_row['plan_date']
                if p_date in ontime_days_dict:
                    ontime_days_dict[p_date]["total"] = ontime_row['total']
                    ontime_days_dict[p_date]["on_time_ok"] = ontime_row['on_time_ok']
            
            for d in sorted(ontime_days_dict.keys()):
                day_data = ontime_days_dict[d]
                on_time_rate = round((day_data["on_time_ok"] / day_data["total"] * 100), 1) if day_data["total"] > 0 else 0
                last_7_days_on_time.append({"date": d.strftime("%m-%d"), "rate": on_time_rate})
        else:
            # 使用全量数据（仅工作日）
            for d in workdays:
                if d in days_dict:
                    day_data = days_dict[d]
                    on_time_rate = round((day_data["on_time_ok"] / day_data["total"] * 100), 1) if day_data["total"] > 0 else 0
                    last_7_days_on_time.append({"date": d.strftime("%m-%d"), "rate": on_time_rate})
        
        # ===================== 3.8 构建返回结果 =====================
        return  {
            "table_data": table_data,  # 今日执行队列数据
            "summary": {
                "total_count": len(table_data),  # 执行队列总条数
                "overdue": status_counts["已超时"],  # 已超时数量
                "in_progress": status_counts["执行中"],  # 执行中数量
                "pending": status_counts["待开工"],  # 待开工数量
                "today_scheduled": today_scheduled,  # 今日应完工序数量
                "today_completed": today_completed,  # 今日已完成数量
                "today_remaining": today_remaining,  # 今日剩余数量
                "month_beat_rate": month_beat_rate,  # 本月节拍兑现率
                "month_on_time_rate": month_on_time_rate,  # 本月准时开完工率
                "today_beat_rate": today_beat_rate,  # 今日节拍兑现率
                "today_on_time_rate": today_on_time_rate,  # 今日准时开完工率
                "last_7_days_beat": last_7_days_beat,  # 近7日节拍率趋势
                "last_7_days_on_time": last_7_days_on_time  # 近7日准时率趋势
            }
        }
    except Exception as e:
        print(f"从clickhouse中获取数据失败，错误原因为: {e}")
        return {
            "table_data": [], 
            "summary": {
                "total_count": 0,
                "overdue": 0, "in_progress": 0, "pending": 0,
                "today_scheduled": 0, "today_completed": 0, "today_remaining": 0
            }
        }

@get("/")
async def index_html() -> Response:
    """根路径路由，返回静态HTML页面"""
    html_path = Path("static/index.html")
    html_content = html_path.read_text(encoding="utf-8")
    return Response(
        content=html_content,
        media_type="text/html"
    )

app = Litestar(
    route_handlers=[index_html, get_table_data],  # 注册路由处理器
    debug=True,  # 开启调试模式
    static_files_config=[
        StaticFilesConfig(
            path="/static",  # URL路径前缀
            directories=["static"],  # 静态文件目录
            name="static"
        )
    ]
)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=12384)
