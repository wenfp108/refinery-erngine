import os
import sys
from factory import UniversalFactory

# ==========================================
# 🚀 启动器：run_factory.py (适配 V3 新架构)
# ==========================================

def main():
    print("🔥 正在启动 Architect's Alpha 认知加工厂...")
    
    # 1. 检查环境变量
    if not os.environ.get("SILICON_FLOW_KEY"):
        print("❌ 错误: 未检测到 SILICON_FLOW_KEY 环境变量")
        return

    try:
        # 2. 实例化工厂 (它会自动加载 masters 目录下的插件)
        factory = UniversalFactory(masters_path="masters")
        
        # 3. 执行生产任务
        # 新版 Factory 会自动去 raw_signals 表里抓取 Twitter/Poly/Reddit/Github 的数据
        # 不需要手动传递 input_raw 文件了
        factory.process_and_ship(vault_path="../vault")
        
    except Exception as e:
        print(f"❌ 运行期间发生未捕获异常: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
