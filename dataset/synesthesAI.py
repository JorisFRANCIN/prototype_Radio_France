import polars as pl
import pathlib as p
import json

df_pl = pl.scan_csv(p.Path("../dataset") / "transcripts" / "whisper.csv", low_memory=True)

query = pl.SQLContext(frame=df_pl).execute("select * from frame where magnetothequeId = '2023I28506S0275'")
query

query = df_pl.filter(pl.col("magnetothequeId") == "2023I28506S0275").select("segments")
query

seg = query.collect().to_dict(as_series=False)
"".join(s["text"] for s in json.loads(seg["segments"][0]))

text_list = [s['text'] for s in json.loads(seg['segments'][0])]

result_text = "".join(text_list)

print("Data received:\n", result_text)

output_json = {
    "resolution": "full-hd",
    "quality": "high",
    "scenes": []
}

sentences = result_text.split(".")

for i, sentence in enumerate(sentences):
    scene = {
        "comment": f"Scene #{i+1}",
        "transition": {
            "style": "circleopen",
            "duration": 1.5
        },
        "elements": [
            {
                "type": "text",
                "text": sentence.strip(),
                "duration": 10
            }
        ]
    }
    output_json["scenes"].append(scene)

print(json.dumps(output_json, indent=2))
