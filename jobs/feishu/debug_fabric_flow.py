# -*- coding: utf-8 -*-
import os
import sys
import difflib
import importlib.util
from pprint import pprint

TARGET_SKU = "BX484-BK-M"
TARGET_FABRIC_NAME = "013仿棉拉架-优化"
TARGET_FABRIC_CODE = "FAB-KNIT-JER-0019"

PROJECT_ROOT = "/opt/apps/pythondata"
SCRIPT_PATH = "/opt/apps/pythondata/jobs/feishu/generate_fabric_forecast.py"


def load_module(path: str):
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)

    spec = importlib.util.spec_from_file_location("fabric_forecast_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    if not os.path.exists(SCRIPT_PATH):
        print(f"❌ 找不到脚本: {SCRIPT_PATH}")
        sys.exit(1)

    mod = load_module(SCRIPT_PATH)

    # 这里直接写死真实函数名，不再自动猜
    extract_spu = getattr(mod, "extract_spu_from_sku")
    extract_color = getattr(mod, "extract_color_abbr_from_sku")
    get_forecast_data = getattr(mod, "get_forecast_order_data")
    get_fabric_usage = getattr(mod, "get_fabric_price_data")
    get_fabric_params = getattr(mod, "get_fabric_params")
    get_merge_map = getattr(mod, "get_fabric_color_merge_mapping", None)
    get_merged_color = getattr(mod, "get_merged_color_abbr", None)

    print("==== 使用固定函数名 ====")
    print("forecast loader: get_forecast_order_data")
    print("fabric_usage loader: get_fabric_price_data")
    print("fabric_params loader: get_fabric_params")
    print()

    print("==== 开始加载源数据 ====")
    forecast_data = get_forecast_data()
    fabric_usage = get_fabric_usage()
    fabric_params = get_fabric_params()
    fabric_color_merge_map = get_merge_map() if get_merge_map else {}

    print(f"forecast_data 条数: {len(forecast_data)}")
    print(f"fabric_usage 条数: {len(fabric_usage)}")
    print(f"fabric_params 条数: {len(fabric_params)}")
    print(f"fabric_color_merge_map 条数: {len(fabric_color_merge_map)}")
    print()

    forecast_rows = [(k, v) for k, v in forecast_data.items() if k[0] == TARGET_SKU]
    print("==== 1) 检查运营预计下单表 ====")
    if not forecast_rows:
        print(f"❌ 在 forecast_data 里没找到 SKU: {TARGET_SKU}")
        print("结论：它在第一关就没进来。")
        return
    else:
        print(f"✅ 找到 {len(forecast_rows)} 条 forecast 记录：")
        for (sku, stat_date), qty in forecast_rows:
            print(f"  SKU={sku}, 统计日期={stat_date}, 预计下单量={qty}")
    print()

    print("==== 2) 从 SKU 提取 SPU / 颜色 ====")
    spu = extract_spu(TARGET_SKU)
    color_abbr = extract_color(TARGET_SKU)
    print("提取出的 SPU:", spu)
    print("提取出的 颜色缩写:", color_abbr)

    if not spu:
        print("❌ SPU 提取失败")
        return
    if not color_abbr:
        print("❌ 颜色缩写提取失败")
        return
    print()

    print("==== 3) 检查这个 SPU 在 fabric_usage 中的面料 ====")
    spu_fabrics = [(s, f, data) for (s, f), data in fabric_usage.items() if s == spu]
    if not spu_fabrics:
        print(f"❌ SPU={spu} 在 fabric_usage 里没有任何面料")
        print("结论：代码会命中 skipped_no_fabric += 1，然后跳过。")
        return

    print(f"✅ SPU={spu} 共找到 {len(spu_fabrics)} 个面料：")
    all_fabric_names = []
    for _, fabric_name, usage_data in spu_fabrics:
        all_fabric_names.append(fabric_name)
        print({
            "面料": fabric_name,
            "单件用量": usage_data.get("单件用量"),
            "单件损耗": usage_data.get("单件损耗"),
        })
    print()

    print("==== 4) 检查目标面料名是否命中 fabric_usage ====")
    usage_hit = [x for x in spu_fabrics if x[1] == TARGET_FABRIC_NAME]
    if usage_hit:
        print(f"✅ 在 fabric_usage 中精确命中: {TARGET_FABRIC_NAME}")
    else:
        print(f"❌ 在 fabric_usage 中未精确命中: {TARGET_FABRIC_NAME}")
        close_names = difflib.get_close_matches(TARGET_FABRIC_NAME, all_fabric_names, n=10, cutoff=0.3)
        print("可能接近的面料名:", close_names)
    print()

    print("==== 5) 检查目标面料名是否命中 fabric_params ====")
    if TARGET_FABRIC_NAME in fabric_params:
        print(f"✅ 在 fabric_params 中精确命中: {TARGET_FABRIC_NAME}")
        pprint(fabric_params[TARGET_FABRIC_NAME])
    else:
        print(f"❌ 在 fabric_params 中未精确命中: {TARGET_FABRIC_NAME}")
        param_names = list(fabric_params.keys())
        close_names = difflib.get_close_matches(TARGET_FABRIC_NAME, param_names, n=10, cutoff=0.3)
        print("可能接近的面料名:", close_names)
    print()

    print("==== 6) 检查面料编号 ====")
    fabric_code = ""
    if TARGET_FABRIC_NAME in fabric_params:
        fabric_code = str(fabric_params[TARGET_FABRIC_NAME].get("面料编号", "") or "").strip()
        print("参数表里的面料编号:", fabric_code or "(空)")
        if not fabric_code:
            print("❌ 面料编号为空")
        elif TARGET_FABRIC_CODE and fabric_code != TARGET_FABRIC_CODE:
            print(f"⚠️ 面料编号和你预期不一致：代码里是 {fabric_code}，你预期是 {TARGET_FABRIC_CODE}")
    else:
        print("⚠️ 因为 fabric_params 没命中，所以这一步无法判断")
    print()

    print("==== 7) 检查归并后面料颜色编号 ====")
    if fabric_code:
        merged_color_abbr = get_merged_color(fabric_code, color_abbr, fabric_color_merge_map) if get_merged_color else color_abbr
        final_fabric_color_code = f"{fabric_code}-{merged_color_abbr}"
        print("原始颜色缩写:", color_abbr)
        print("归并颜色缩写:", merged_color_abbr)
        print("最终面料颜色编号:", final_fabric_color_code)
    else:
        print("⚠️ 因为 fabric_code 为空，无法生成最终面料颜色编号")
    print()

    print("==== 8) 结论 ====")
    if not forecast_rows:
        print("原因：运营预计下单里没有这条成品 SKU")
    elif not spu:
        print("原因：SKU 无法提取 SPU")
    elif not spu_fabrics:
        print("原因：SPU 在面料核价表里没有任何面料")
    elif not usage_hit:
        print("原因：SPU 虽然有面料，但不是你指定的这个面料名")
    elif TARGET_FABRIC_NAME not in fabric_params:
        print("原因：面料名在面料核价表里有，但在定制面料参数表里没精确命中")
    elif not fabric_code:
        print("原因：定制面料参数表里这条面料的面料编号为空")
    else:
        print("这条链路理论上应该能流转进面料预估表。")
        print("如果仍然没出现，就继续检查唯一键覆盖或你查的统计日期。")


if __name__ == "__main__":
    main()
