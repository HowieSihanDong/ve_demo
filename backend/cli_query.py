#!/usr/bin/env python3
"""
AI 数据库查询终端版 - 纯命令行交互
支持：MySQL 查询、AI 生成 SQL、Redis 缓存
"""

import os
import sys
import mysql.connector
import redis
import json
from datetime import datetime
from dotenv import load_dotenv
from llm_service import get_sql_from_llm

# 加载环境变量
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', '.env')
load_dotenv(env_path)

# MySQL 配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'demo_db'),
    'charset': 'utf8mb4'
}

# Redis 配置
def get_redis_client():
    """获取 Redis 连接"""
    try:
        client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=int(os.getenv('REDIS_DB', 0)),
            decode_responses=True
        )
        client.ping()
        return client
    except Exception as e:
        print(f"⚠️  Redis 未连接: {e}")
        return None

# 内存缓存（Redis 不可用时的备用）
mock_cache = {}

def get_db_connection():
    """获取 MySQL 连接"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌  MySQL 连接失败: {e}")
        return None

def execute_sql(sql):
    """执行 SQL 查询"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    except Exception as e:
        print(f"❌  SQL 执行失败: {e}")
        return None

def query_with_cache(prompt):
    """带缓存的查询"""
    redis_client = get_redis_client()
    cache_key = f"cache:{prompt}"
    sql = None
    cache_hit = False
    
    # 1. 检查缓存
    if redis_client:
        try:
            sql = redis_client.get(cache_key)
            if sql:
                cache_hit = True
                print("🚀  [Redis 缓存命中]")
        except Exception as e:
            print(f"⚠️  Redis 读取失败: {e}")
    elif prompt in mock_cache:
        sql = mock_cache[prompt]
        cache_hit = True
        print("📦  [内存缓存命中]")
    
    # 2. 未命中缓存，调用 AI
    if not sql:
        print("🤖  [AI 生成 SQL...]")
        try:
            sql = get_sql_from_llm(prompt)
            print(f"📄  [生成 SQL] {sql}")
        except Exception as e:
            print(f"❌  AI 调用失败: {e}")
            return None
        
        # 存入缓存
        if redis_client:
            try:
                redis_client.setex(cache_key, 3600, sql)
                print("💾  [已缓存到 Redis]")
            except Exception as e:
                print(f"⚠️  Redis 写入失败: {e}")
        else:
            mock_cache[prompt] = sql
            print("💾  [已缓存到内存]")
    else:
        print(f"📄  [缓存 SQL] {sql}")
    
    # 3. 执行查询
    print("🔍  [执行查询...]")
    results = execute_sql(sql)
    
    if results is None:
        return None
    
    return {
        'sql': sql,
        'data': results,
        'cache_hit': cache_hit,
        'count': len(results)
    }

def print_results(results):
    """打印查询结果"""
    if results is None:
        print("⚠️  查询失败，无数据返回")
        return
    
    if not results.get('data'):
        print("📭  查询结果为空")
        return
    
    data = results['data']
    
    # 获取列名
    columns = list(data[0].keys())
    
    # 计算列宽
    col_widths = {}
    for col in columns:
        header_len = len(str(col))
        max_data_len = max([len(str(row.get(col, ''))) for row in data])
        col_widths[col] = max(header_len, max_data_len) + 2
    
    # 打印分隔线
    total_width = sum(col_widths.values()) + len(columns) + 1
    print("=" * total_width)
    
    # 打印表头
    header = "|"
    for col in columns:
        header += f" {str(col):^{col_widths[col]-2}} |"
    print(header)
    print("=" * total_width)
    
    # 打印数据
    for row in data:
        line = "|"
        for col in columns:
            val = str(row.get(col, ''))[:col_widths[col]-2]
            line += f" {val:<{col_widths[col]-2}} |"
        print(line)
    
    print("=" * total_width)
    print(f"📊  共 {results['count']} 条记录 {'(来自缓存)' if results['cache_hit'] else ''}")

def show_help():
    """显示帮助信息"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║           🤖 AI 数据库查询助手 - 终端版                       ║
╠══════════════════════════════════════════════════════════════╣
║  使用方法:                                                    ║
║    直接输入自然语言查询，例如:                                ║
║    - 查询所有项目                                            ║
║    - 查看张三负责的项目                                      ║
║    - 预算大于10万的项目                                      ║
║    - 已交付的项目有哪些                                      ║
╠══════════════════════════════════════════════════════════════╣
║  特殊命令:                                                    ║
║    /help     - 显示帮助                                      ║
║    /sql      - 直接执行 SQL 语句                             ║
║    /tables   - 查看所有表                                    ║
║    /schema   - 查看表结构                                    ║
║    /quit     - 退出程序                                      ║
╚══════════════════════════════════════════════════════════════╝
    """)

def show_tables():
    """显示所有表"""
    results = execute_sql("SHOW TABLES")
    if results:
        print("📋  数据库中的表:")
        for row in results:
            print(f"   • {list(row.values())[0]}")

def show_schema():
    """显示表结构"""
    print("📐  ai_projects 表结构:")
    results = execute_sql("DESCRIBE ai_projects")
    if results:
        for row in results:
            print(f"   • {row['Field']}: {row['Type']} {row['Null']} {row['Key']}")

def direct_sql():
    """直接执行 SQL"""
    sql = input("📝  请输入 SQL 语句: ").strip()
    if not sql:
        return
    
    print(f"🔍  执行: {sql}")
    results = execute_sql(sql)
    
    if results is not None:
        print_results({
            'sql': sql,
            'data': results,
            'cache_hit': False,
            'count': len(results)
        })

def main():
    """主程序"""
    show_help()
    
    # 测试数据库连接
    conn = get_db_connection()
    if conn:
        print("✅  MySQL 连接成功!")
        conn.close()
    else:
        print("❌  MySQL 连接失败，请检查配置")
        return
    
    # 测试 Redis
    redis_client = get_redis_client()
    if redis_client:
        print("✅  Redis 连接成功!")
    else:
        print("⚠️   Redis 未连接，将使用内存缓存")
    
    print("\n" + "="*60)
    
    while True:
        try:
            # 获取用户输入
            user_input = input("\n💬  请输入查询 (或 /help 查看帮助): ").strip()
            
            if not user_input:
                continue
            
            # 处理特殊命令
            if user_input.lower() in ['/quit', '/exit', 'quit', 'exit']:
                print("👋  再见!")
                break
            
            if user_input.lower() == '/help':
                show_help()
                continue
            
            if user_input.lower() == '/tables':
                show_tables()
                continue
            
            if user_input.lower() == '/schema':
                show_schema()
                continue
            
            if user_input.lower() == '/sql':
                direct_sql()
                continue
            
            # 普通查询
            print(f"\n🔎  正在查询: {user_input}")
            print("-" * 60)
            
            results = query_with_cache(user_input)
            
            if results:
                print_results(results)
            else:
                print("❌  查询失败")
                
        except KeyboardInterrupt:
            print("\n\n👋  再见!")
            break
        except Exception as e:
            print(f"❌  错误: {e}")

if __name__ == "__main__":
    main()
