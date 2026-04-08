# ============================================================
# 导入模块
# ============================================================
from pathlib import Path  # 用于处理文件路径
import mimetypes  # 用于处理文件的MIME类型
import os  # 用于读取环境变量
import clickhouse_connect  # ClickHouse数据库连接驱动
from litestar import Litestar, get  # Litestar Web框架
from litestar.static_files.config import StaticFilesConfig  # 静态文件配置
from litestar.response import Response  # 响应对象
from datetime import datetime, time, timedelta, date  # 日期时间处理
import holidays  # 中国法定节假日

# ============================================================
# 1. 配置部分
# ============================================================

# 明确添加 .js 文件的 MIME 类型，确保浏览器能正确识别脚本
mimetypes.add_type("application/javascript", ".js")

# ClickHouse 连接配置 (从环境变量获取，默认为本地配置)
# - CH_HOST: 数据库主机地址
# - CH_PORT: 数据库端口 (默认8123)
# - CH_USER: 数据库用户名
# - CH_PASSWORD: 数据库密码
# - CH_DATABASE: 默认数据库名称
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "cheakf")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "Swq8855830.")
CH_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "dwd")

# ============================================================
# 2. 工具函数部分
# ============================================================

def get_ch_client():
    """获取ClickHouse数据库连接客户端"""
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE
    )

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

from collections import Counter

# ============================================================
# 3. API 接口部分
# ============================================================

@get("/api/table-data")
async def get_table_data(team: str = None, team2: str = None) -> dict:
    """
    获取表格数据的主API接口
    参数:
        - team: 班组名称过滤（用于节拍兑现率趋势）
        - team2: 班组名称过滤（用于准时开完工率趋势）
    返回:
        - table_data: 今日执行队列数据
        - summary: 汇总统计数据
    """
    try:
        # 获取ClickHouse数据库连接客户端
        client = get_ch_client()
        
        # ===================== 3.1 主查询 =====================
        # 查询当前月及近7天的生产计划数据
        # 包含字段：项目号、车号、节车号、排程时间、计划时间、实际时间、班组、是否兑现节拍、是否准时开完工
        query = """
            SELECT 
                today() as ch_today,  -- 获取数据库当前日期
                BILL.`项目号` , 
                BILL.`车号` , 
                BILL.`节车号` , 
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
                (toStartOfMonth(toDate(BILL.`计划开始时间`)) = toStartOfMonth(today())  -- 本月数据
                OR toDate(BILL.`实际结束时间`) = today()  -- 今日完工数据
                OR (toDate(BILL.`计划开始时间`) >= today() - INTERVAL 6 DAY AND toDate(BILL.`计划开始时间`) <= today()))  -- 近7天计划
            ORDER BY 
                BILL.`计划开始时间` DESC
        """
        result = client.query(query)

        # ===================== 3.2 节拍兑现率趋势查询 =====================
        # 查询近7天的节拍兑现率，按日期分组
        team_filter = f"AND BILL.`班组名称` = '{team}'" if team else ""
        trend_query = f"""
            SELECT 
                toDate(BILL.`计划开始时间`) as plan_date,  -- 计划开工日期
                COUNT(*) as total,  -- 总数
                sum(if(BILL.`是否兑现节拍` = '是', 1, 0)) as beat_ok  -- 节拍达标数
            FROM 
                dwd.beat_fulfillment_rate BILL 
            WHERE 
                toDate(BILL.`计划开始时间`) >= today() - INTERVAL 6 DAY
                AND toDate(BILL.`计划开始时间`) <= today()
                {team_filter}
            GROUP BY toDate(BILL.`计划开始时间`)
            ORDER BY plan_date
        """
        trend_result = client.query(trend_query)
        
        # ===================== 3.3 准时开完工率趋势查询 =====================
        # 查询近7天的准时开完工率，按日期分组
        team2_filter = f"AND BILL.`班组名称` = '{team2}'" if team2 else ""
        ontime_trend_query = f"""
            SELECT 
                toDate(BILL.`计划开始时间`) as plan_date,  -- 计划开工日期
                COUNT(*) as total,  -- 总数
                sum(if(BILL.`是否准时开完工` = '是', 1, 0)) as on_time_ok  -- 准时数
            FROM 
                dwd.beat_fulfillment_rate BILL 
            WHERE 
                toDate(BILL.`计划开始时间`) >= today() - INTERVAL 6 DAY
                AND toDate(BILL.`计划开始时间`) <= today()
                {team2_filter}
            GROUP BY toDate(BILL.`计划开始时间`)
            ORDER BY plan_date
        """
        ontime_trend_result = client.query(ontime_trend_query)
        
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

        # 使用本地 today() 进行日期比较，避免时区差异
        local_today = date.today()
        print(f"DEBUG: local_today = {local_today}")

        # 调试：检查数据处理前的准备
        if not result.result_rows:
            print("DEBUG: 查询结果为空")

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
            
            if has_actual_start and not has_actual_end:
                # 执行中: 实际开工时间存在且实际结束时间不存在
                status = "执行中"
                is_pending = False
                if plan_end and now > plan_end:
                    # 已超时: 当前时间 > 计划结束时间
                    status = "已超时"
                    is_overtime = True
            
            # ===================== 统计本月指标 =====================
            # 严格按照用户要求：统计计划开始时间在本月的数据
            if plan_start and plan_start.year == local_today.year and plan_start.month == local_today.month:
                month_total += 1
                if row['是否兑现节拍'] == '是':
                    month_beat_ok += 1
                if row['是否准时开完工'] == '是':
                    month_on_time_ok += 1

            # ===================== 统计今日概况 =====================
            # 应完工序数量：统计计划开工时间为今日的数据数量
            if plan_start and plan_start.date() == local_today:
                today_scheduled += 1
                if row['是否兑现节拍'] == '是':
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

                # ===================== 状态颜色映射 =====================
                # 根据状态类型返回对应的CSS类名，用于前端显示
                status_map = {
                    "已超时": "bg-primary-container text-white",
                    "执行中": "bg-[#00C853] text-white",
                    "待开工": "bg-surface-container-highest text-on-surface"
                }

                table_data.append({
                    "status": status,
                    "status_class": status_map.get(status, ""),
                    "project": row['项目号'],
                    "train_no": row['车号'],
                    "car_no": row['节车号'],
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
        # 如果选择了 team2，则使用独立查询的 ontime_trend_result
        # 否则使用全量数据 days_dict
        if team2:
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
        # ===================== 3.9 异常处理 =====================
        print(f"Error fetching data from ClickHouse: {e}")
        return {
            "table_data": [], 
            "summary": {
                "total_count": 0,
                "overdue": 0, "in_progress": 0, "pending": 0,
                "today_scheduled": 0, "today_completed": 0, "today_remaining": 0
            }
        }

# ============================================================
# 4. 静态页面路由
# ============================================================

@get("/")
async def index_html() -> Response:
    """根路径路由，返回静态HTML页面"""
    html_path = Path("static/index.html")
    html_content = html_path.read_text(encoding="utf-8")
    return Response(
        content=html_content,
        media_type="text/html"
    )

# ============================================================
# 5. 应用启动配置
# ============================================================

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

# ===================== 主程序入口 =====================
if __name__ == "__main__":
    # 启动 uvicorn 服务器
    # - host="0.0.0.0": 监听所有网络接口
    # - port=12386: 监听端口号
    # - reload=True: 开启热重载，修改代码后自动重启
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=12386, reload=True)
