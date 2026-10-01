BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = {
    "playback": "playback-events",
    "metadata": "content-metadata",
    "subscription": "subscription-events",
    "cdn": "cdn-performance",
    "social": "social-mentions",
}

REGIONS = ["North", "South", "East", "West"]

TITLES = [
    {
        "title_id": "T001",
        "title": "The Last Signal",
        "genre": "Thriller",
        "content_duration_sec": 7200,
    },
    {
        "title_id": "T002",
        "title": "Campus Diaries",
        "genre": "Drama",
        "content_duration_sec": 5400,
    },
    {
        "title_id": "T003",
        "title": "Cricket Nights",
        "genre": "Sports",
        "content_duration_sec": 6600,
    },
    {
        "title_id": "T004",
        "title": "Laugh Track Live",
        "genre": "Comedy",
        "content_duration_sec": 4800,
    },
    {
        "title_id": "T005",
        "title": "Galaxy 9",
        "genre": "Sci-Fi",
        "content_duration_sec": 8400,
    },
]

USERS = [f"U{i:04d}" for i in range(1, 101)]

DEVICES = ["mobile", "smart_tv", "laptop", "tablet"]

PLAYBACK_EVENTS = ["play", "pause", "seek", "buffer", "stop"]