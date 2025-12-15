<?php
/**
 * FINAL import.php
 * Compatible with UUID-based song_session_id schema
 * Includes execution time measurement
 */

ini_set('memory_limit', '512M');
set_time_limit(0);

$startTime = microtime(true);

// ---------------- CONFIG ----------------
$dbHost = 'localhost';
$dbName = 'radioDB';
$dbUser = 'root';
$dbPass = '';
$jsonFile = __DIR__ . '/silver_enrich_durationsec_genresOK2.json';
// ----------------------------------------

// ---------------- PDO -------------------
$dsn = "mysql:host=$dbHost;dbname=$dbName;charset=utf8mb4";
$pdo = new PDO($dsn, $dbUser, $dbPass, [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
]);
// ----------------------------------------

// ---------------- LOAD JSON -------------
$data = json_decode(file_get_contents($jsonFile), true);
if (!$data) {
    die('JSON file could not be loaded or is invalid');
}
// ----------------------------------------

// ---------------- PREPARED STATEMENTS ---
$selectGenre = $pdo->prepare("SELECT id FROM genre WHERE genre = ?");
$insertGenre = $pdo->prepare("INSERT INTO genre (genre) VALUES (?)");

$selectRadio = $pdo->prepare("SELECT id FROM radio WHERE name = ?");
$insertRadio = $pdo->prepare("INSERT INTO radio (name, headquarters, genre_id) VALUES (?, ?, ?)");

$insertSongSession = $pdo->prepare(
    "INSERT IGNORE INTO song_session (song_session_id) VALUES (?)"
);

$selectTime = $pdo->prepare(
    "SELECT id FROM time WHERE date = ? AND hour = ? AND minute = ? AND second = ?"
);
$insertTime = $pdo->prepare(
    "INSERT INTO time (hour, minute, second, date, day_week) VALUES (?, ?, ?, ?, ?)"
);

$insertSong = $pdo->prepare(
    "INSERT INTO song (title, artists, duration, release_year, genre_id, song_session_id)
     VALUES (?, ?, ?, ?, ?, ?)"
);

$insertPlaySession = $pdo->prepare(
    "INSERT INTO play_session (listener_count, song_session_id, radio_id, time_id)
     VALUES (?, ?, ?, ?)"
);
// ----------------------------------------

// ---------------- ID MAPS ---------------
$genreMap = [];
$radioMap = [];
$timeMap = [];
// ----------------------------------------

try {
    $pdo->beginTransaction();

    foreach ($data as $row) {

        // -------- GENRE --------
        $genreName = trim($row['genre']);
        if (!isset($genreMap[$genreName])) {
            $selectGenre->execute([$genreName]);
            $gid = $selectGenre->fetchColumn();
            if (!$gid) {
                $insertGenre->execute([$genreName]);
                $gid = $pdo->lastInsertId();
            }
            $genreMap[$genreName] = $gid;
        }
        $genreId = $genreMap[$genreName];

        // -------- RADIO --------
        $radioName = trim($row['radio']);
        if (!isset($radioMap[$radioName])) {
            $selectRadio->execute([$radioName]);
            $rid = $selectRadio->fetchColumn();
            if (!$rid) {
                $insertRadio->execute([$radioName, 'unknown', $genreId]);
                $rid = $pdo->lastInsertId();
            }
            $radioMap[$radioName] = $rid;
        }
        $radioId = $radioMap[$radioName];

        // -------- SONG SESSION (UUID) --------
        $songSessionId = $row['song_session_id'];
        $insertSongSession->execute([$songSessionId]);

        // -------- TIME --------
        [$h, $m, $s] = array_map('intval', explode(':', $row['time']));
        $dateObj = DateTime::createFromFormat('d.m.Y', $row['date']);
        $dateSql = $dateObj->format('Y-m-d');
        $dayWeek = (int)$dateObj->format('N');

        $timeKey = "$dateSql $h:$m:$s";
        if (!isset($timeMap[$timeKey])) {
            $selectTime->execute([$dateSql, $h, $m, $s]);
            $tid = $selectTime->fetchColumn();
            if (!$tid) {
                $insertTime->execute([$h, $m, $s, $dateSql, $dayWeek]);
                $tid = $pdo->lastInsertId();
            }
            $timeMap[$timeKey] = $tid;
        }
        $timeId = $timeMap[$timeKey];

        // -------- SONG --------
        $title = trim($row['title']);
        $artists = implode(', ', $row['artists']);
        $durationSeconds = is_numeric($row['duration'])
            ? (int)$row['duration']
            : null;
        $releaseYear = isset($row['release_year']) && is_numeric($row['release_year'])
            ? (int)$row['release_year']
            : null;

        $insertSong->execute([
            $title,
            $artists,
            $durationSeconds,
            $releaseYear,
            $genreId,
            $songSessionId
        ]);

        // -------- PLAY SESSION --------
        $insertPlaySession->execute([
            null,
            $songSessionId,
            $radioId,
            $timeId
        ]);
    }

    $pdo->commit();

    $elapsed = microtime(true) - $startTime;
    echo "Import completed successfully in " . round($elapsed, 2) . " seconds";

} catch (Exception $e) {
    $pdo->rollBack();
    echo "Import failed: " . $e->getMessage();
}
