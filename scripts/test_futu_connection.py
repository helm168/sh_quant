"""
富途 OpenAPI 最小连接验证脚本。

目标：搞清楚之前 connection 断开的根因。不拉历史数据、不订阅推送，
只做 connect → login → 一次同步请求 → close 的完整握手循环，重复 30 次
（每次间隔 10 秒），看是否能稳定运行。

前置条件：
  1. 已下载并启动 FutuOpenD（GUI 或 CLI 版）
     - GUI 下载: https://www.futunn.com/download/openAPI
     - 启动后看到「连接服务器成功」+「登录账号成功」
  2. 行情权限已激活
     - 富途 App → 行情 → 升级 → Lv1 港股（免费）确认已开通
  3. 同时不要开富途 App / 富途牛牛 App（会顶号）
  4. pip install futu-api （或 uv pip install futu-api）

用法：
    source .venv/bin/activate
    pip install futu-api
    python scripts/test_futu_connection.py

输出：
    每 10 秒一次 ping，记录成功/失败 + 用时。
    跑 5 分钟还没断，说明 OpenD 设置健康，可以进下一步建 vendor。
"""

from __future__ import annotations

import sys
import time
from datetime import datetime

try:
    from futu import OpenQuoteContext, RET_OK, KLType, AuType
except ImportError:
    sys.exit('futu-api 没装。先 pip install futu-api')


# OpenD 默认 host/port
HOST = '127.0.0.1'
PORT = 11111

# 30 次 × 10 秒 = 5 分钟，覆盖一般断连周期
PING_COUNT = 30
PING_INTERVAL = 10

# 用腾讯 0700 做 ping，因为它的数据肯定存在
TEST_SYMBOL = 'HK.00700'


def ping_once(ctx: OpenQuoteContext, i: int) -> tuple[bool, str]:
    """做一次同步请求作为存活探针。"""
    t0 = time.time()
    # request_history_kline 是最简单的同步请求
    ret, data = ctx.request_history_kline(
        TEST_SYMBOL,
        start=datetime.now().strftime('%Y-%m-%d'),
        end=datetime.now().strftime('%Y-%m-%d'),
        ktype=KLType.K_DAY,
        autype=AuType.NONE,
        max_count=1,
    )
    elapsed_ms = int((time.time() - t0) * 1000)
    if ret == RET_OK:
        return True, f'ok ({elapsed_ms}ms, rows={len(data) if hasattr(data, "__len__") else "?"})'
    return False, f'fail ret={ret} data={data}'


def main() -> None:
    print(f'=== FutuOpenD 连接稳定性测试 ===')
    print(f'host={HOST}:{PORT}  symbol={TEST_SYMBOL}')
    print(f'ping {PING_COUNT} 次，每次间隔 {PING_INTERVAL} 秒（总时长 {PING_COUNT * PING_INTERVAL // 60} 分钟）')
    print('=' * 60)

    # 1) 建连
    try:
        ctx = OpenQuoteContext(host=HOST, port=PORT)
    except Exception as e:
        sys.exit(
            f'OpenQuoteContext 创建失败: {e}\n'
            '→ 大概率是 OpenD 没启动，或端口被占用。'
            '\n→ 检查：(1) OpenD 进程在不在；(2) lsof -i :11111 有没有 OpenD'
        )

    success_count = 0
    fail_count = 0
    last_fail_reason: str | None = None

    try:
        for i in range(1, PING_COUNT + 1):
            t = datetime.now().strftime('%H:%M:%S')
            try:
                ok, msg = ping_once(ctx, i)
            except Exception as e:
                ok, msg = False, f'exception: {e}'

            if ok:
                success_count += 1
                tag = 'OK'
            else:
                fail_count += 1
                last_fail_reason = msg
                tag = 'FAIL'

            print(f'[{t}] ping #{i:>2}  {tag}  {msg}')

            if i < PING_COUNT:
                time.sleep(PING_INTERVAL)
    finally:
        try:
            ctx.close()
        except Exception:
            pass

    print('=' * 60)
    print(f'总计: 成功 {success_count}/{PING_COUNT}, 失败 {fail_count}')
    if last_fail_reason:
        print(f'最后失败原因: {last_fail_reason}')

    # 给出诊断结论
    print()
    if success_count == PING_COUNT:
        print('✓ 连接稳定，OpenD 设置健康。可以进入下一步：写 futu_stock.py vendor。')
    elif success_count > PING_COUNT * 0.8:
        print('△ 大部分成功但偶发断连。可能：')
        print('  - 网络抖动')
        print('  - heartbeat 边缘 case → 升级 futu-api 到最新版')
    elif success_count > 0:
        print('✗ 频繁断连。检查清单：')
        print('  1. 富途 App / 牛牛 App 是不是开着？（顶号导致断）')
        print('  2. OpenD 里看「行情订阅状态」是否绿色')
        print('  3. Mac 是不是要进休眠？（合盖或 idle）')
    else:
        print('✗ 完全连不上。检查清单：')
        print('  1. OpenD 是不是真的启动了？打开 OpenD 看「连接服务器」状态')
        print('  2. OpenD 里有没有「登录账号成功」？没登录拿不到数据')
        print('  3. 端口是不是 11111？换过的话改本脚本 PORT')
        print('  4. 行情权限：富途 App → 行情 → 升级，确认 Lv1 港股已开通')


if __name__ == '__main__':
    main()
