#!/usr/bin/env python3
"""
CSVファイルを読み込んで、英語名ごとに学名をグループ化したJSONLファイルを生成する
"""

import csv
import json
from collections import defaultdict
from pathlib import Path


def csv_to_jsonl(
    input_csv: str = "scientific_name_en_common_name.csv",
    output_jsonl: str = "english_name_grouped.jsonl"
):
    """
    CSVファイルを読み込んで、英語名ごとに学名をグループ化したJSONLを生成
    
    Args:
        input_csv: 入力CSVファイルパス
        output_jsonl: 出力JSONLファイルパス
    """
    # 英語名をキーとして学名をリストで格納
    en_name_to_scientific_names = defaultdict(list)
    
    # CSVを読み込む
    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            scientific_name = row['scientific_name']
            english_common_name = row['english_common_name']
            en_name_to_scientific_names[english_common_name].append(scientific_name)
    
    # JSONLファイルに出力
    with open(output_jsonl, 'w', encoding='utf-8') as f:
        for english_common_name, scientific_name_list in sorted(en_name_to_scientific_names.items()):
            json_obj = {
                "english_common_name": english_common_name,
                "scientific_name_list": scientific_name_list
            }
            f.write(json.dumps(json_obj, ensure_ascii=False) + '\n')
    
    # 統計情報を表示
    print(f"処理完了:")
    print(f"  入力: {input_csv}")
    print(f"  出力: {output_jsonl}")
    print(f"  英語名の種類: {len(en_name_to_scientific_names)}")
    print(f"  総学名数: {sum(len(names) for names in en_name_to_scientific_names.values())}")


if __name__ == "__main__":
    csv_to_jsonl()
