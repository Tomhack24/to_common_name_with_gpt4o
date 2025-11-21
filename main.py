import os
import csv
import time
import re
import argparse
import asyncio
from pathlib import Path
from openai import AsyncOpenAI
from dotenv import load_dotenv
from pydantic import BaseModel

# 環境変数を読み込み
load_dotenv()

# OpenAI 非同期クライアントの初期化
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))


class CommonNameResponse(BaseModel):
    """構造化された応答モデル（英語用）"""
    common_name: str


class JapaneseCommonNameResponse(BaseModel):
    """構造化された応答モデル（日本語用）"""
    呼称: str


def load_prompt_template(prompt_path: str) -> str:
    """プロンプトテンプレートを読み込む"""
    with open(prompt_path, 'r', encoding='utf-8') as f:
        return f.read()


def load_species_list(species_path: str) -> list[str]:
    """学名リストを読み込む"""
    with open(species_path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]


def clean_common_name(common_name: str, language: str = "en") -> str:
    """コモンネームをクリーンアップする後処理"""
    # 前後の空白を削除
    common_name = common_name.strip()

    if language == "ja":
        # 日本語の場合の後処理
        # 「呼称:」「呼称：」などのプレフィックスを除去
        patterns = [
            r'^呼称[:：]\s*',
            r'^日本語名[:：]\s*',
        ]
        for pattern in patterns:
            common_name = re.sub(pattern, '', common_name)
        
        # 改行や余分な説明文を除去
        if '\n' in common_name:
            common_name = common_name.split('\n')[0].strip()
        
        # カギ括弧や引用符を除去
        common_name = common_name.strip('「」『』""\'"')
        
    else:
        # 英語の場合の後処理
        # "Common Name:" などのプレフィックスを除去
        patterns = [
            r'^Common Name:\s*',
            r'^Name:\s*',
        ]
        for pattern in patterns:
            common_name = re.sub(pattern, '', common_name, flags=re.IGNORECASE)
        
        # 改行や余分な説明文を除去
        if '\n' in common_name:
            common_name = common_name.split('\n')[0].strip()
        
        # 引用符を除去
        common_name = common_name.strip('"\'')
    
    # 最終的な空白整理
    common_name = common_name.strip()
    
    return common_name


async def get_common_name(species: str, prompt_template: str, model: str = "gpt-4o", language: str = "en", max_retries: int = 10) -> str:
    """GPTを使って学名からコモンネームを取得(構造化された出力)"""
    # プロンプトテンプレートの[species]を実際の学名に置き換え
    prompt = prompt_template.replace('[species]', species)
    
    # 言語に応じて適切なレスポンスフォーマットを選択
    if language == "ja":
        response_format = JapaneseCommonNameResponse
    else:
        response_format = CommonNameResponse
    
    for attempt in range(max_retries):
        try:
            response = await client.beta.chat.completions.parse(
                model=model,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                response_format=response_format,
                max_tokens=200,
                temperature=0.0,
            )
            
            parsed_response = response.choices[0].message.parsed
            if parsed_response:
                if language == "ja":
                    common_name = parsed_response.呼称
                else:
                    common_name = parsed_response.common_name
            else:
                common_name = "エラー"
            
            # 後処理でクリーンアップ
            common_name = clean_common_name(common_name, language)
            
            return common_name
        
        except Exception as e:
            error_message = str(e)
            if "429" in error_message or "rate_limit" in error_message.lower():
                wait_time = 10 * (attempt + 1)  # 徐々に待機時間を増やす
                print(f"  Rate limit error. Waiting {wait_time} seconds before retry (attempt {attempt + 1}/{max_retries})...")
                await asyncio.sleep(wait_time)
                if attempt == max_retries - 1:
                    print(f"Error processing {species}: {e}")
                    return "エラー"
            else:
                print(f"Error processing {species}: {e}")
                return "エラー"
    
    return "エラー"


async def process_species(line_number: int, species: str, en_prompt_template: str, ja_prompt_template: str) -> dict:
    """1つの学名を処理して結果を返す"""
    print(f"処理中 ({line_number}): {species}")
    
    # 英語と日本語のコモンネームを並列取得
    en_common_name, ja_common_name = await asyncio.gather(
        get_common_name(species, en_prompt_template, language="en"),
        get_common_name(species, ja_prompt_template, language="ja")
    )
    
    print(f"  完了 ({line_number}): EN={en_common_name}, JA={ja_common_name}")
    
    return {
        'number': line_number,
        'scientific_name': species,
        'english_common_name': en_common_name,
        'japanese_common_name': ja_common_name
    }


async def process_and_save_batch(species_list: list[str], start_line: int, end_line: int, 
                                en_prompt_template: str, ja_prompt_template: str, 
                                output_path: str, file_mode: str,
                                batch_size: int = 10):
    """バッチで並列処理し、バッチごとにCSVに保存"""
    tasks = []
    for i in range(start_line - 1, end_line):
        species = species_list[i]
        line_number = i + 1
        tasks.append((line_number, process_species(line_number, species, en_prompt_template, ja_prompt_template)))
    
    # CSVファイルを開く（追記モードまたは新規作成）
    with open(output_path, file_mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['number', 'scientific_name', 'english_common_name', 'japanese_common_name'])
        
        # 新規作成の場合のみヘッダーを書き込み
        if file_mode == 'w':
            writer.writeheader()
            f.flush()
        
        # バッチサイズごとに処理して保存
        for i in range(0, len(tasks), batch_size):
            batch = [task for _, task in tasks[i:i + batch_size]]
            print(f"\nバッチ処理中: {i + 1} - {min(i + batch_size, len(tasks))} / {len(tasks)}")
            batch_results = await asyncio.gather(*batch)
            
            # 行番号順にソートして保存
            batch_results.sort(key=lambda x: x['number'])
            
            for row in batch_results:
                writer.writerow(row)
            
            f.flush()  # バッファをフラッシュして即座にディスクに書き込む
            print(f"  バッチ保存完了: {len(batch_results)} 件")
            
            # バッチ間で少し待機（レート制限対策）
            if i + batch_size < len(tasks):
                await asyncio.sleep(2)


async def main_async():
    # argparseを使用してコマンドライン引数を解析
    parser = argparse.ArgumentParser(description='学名からコモンネームを取得してCSVに保存します')
    parser.add_argument('--start', type=int, default=1, help='処理を開始する行番号 (デフォルト: 1)')
    parser.add_argument('--line', type=int, help='指定した行番号のみを処理')
    parser.add_argument('--batch-size', type=int, default=10, help='並列処理のバッチサイズ (デフォルト: 10)')
    args = parser.parse_args()
    
    start_line = args.start
    single_line = args.line
    batch_size = args.batch_size
    
    # ファイルパスの設定
    en_prompt_path = "prompts/en-prompt.txt"
    ja_prompt_path = "prompts/ja-prompt.txt"
    species_path = "mammal_species_confirmed.txt"
    output_path = "jp_en_common_name.csv"
    
    # プロンプトテンプレートを読み込み
    print("プロンプトテンプレートを読み込み中...")
    en_prompt_template = load_prompt_template(en_prompt_path)
    ja_prompt_template = load_prompt_template(ja_prompt_path)
    
    print("学名リストを読み込み中...")
    species_list = load_species_list(species_path)
    print(f"合計 {len(species_list)} 件の学名")
    
    # 単一行処理の場合
    if single_line is not None:
        print(f"指定行 {single_line} のみを処理します")
        file_mode = 'a'
        start_line = single_line
        end_line = single_line
    else:
        # 既存のCSVファイルをチェック
        file_mode = 'w'
        if start_line > 1 and os.path.exists(output_path):
            file_mode = 'a'
            print(f"既存のCSVファイルに追記します。{start_line}行目から開始...")
        
        print(f"開始行: {start_line}")
        end_line = len(species_list)
    
    # 並列処理で全データを取得し、バッチごとに保存
    print(f"\n並列処理開始 (バッチサイズ: {batch_size})")
    await process_and_save_batch(species_list, start_line, end_line, 
                                 en_prompt_template, ja_prompt_template, 
                                 output_path, file_mode, batch_size)
    
    print(f"\n完了! {end_line - start_line + 1} 件のデータを処理しました")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
