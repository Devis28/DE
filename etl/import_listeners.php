<?php
/**
 * import_listeners.php (FINAL – correct data model)
 * -------------------------------------------------
 * - Keeps existing DB structure
 * - Adds NEW play_session rows per measurement
 * - Uses recorded_at to distinguish measurements
 * - Copies radio_id + time_id from existing play_session
 * - Streams JSON (memory safe)
 */

ini_set('memory_limit', '1024M');
set_time_limit(0);

$startTime = microtime(true);

// ---------------- CONFIG ----------------
$dbHost = 'localhost';
$dbName = 'radioDB';
$dbUser = 'root';
$dbPass = '';
$jsonFile = __DIR__ . '/merged_listeners_v2.json';
// ----------------------------------------

// ---------------- PDO -------------------
$dsn = "mysql:host=$dbHost;dbname=$dbName;charset=utf8mb4";
$pdo = new PDO($dsn, $dbUser, $dbPass, [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
]);
// ----------------------------------------

// ---------------- PREPARED STATEMENTS ---
// find base play_session (radio + time)
$getBaseSession = $pdo->prepare(
    "SELECT radio_id, time_id
     FROM play_session
     WHERE song_session_id = ?
     LIMIT 1"
);

// insert new listener measurement
$insertPlaySession = $pdo->prepare(
    "INSERT INTO play_session
        (listener_count, song_session_id, radio_id, time_id, recorded_at)
     VALUES (?, ?, ?, ?, ?)"
);
// ----------------------------------------

$inserted = 0;
$skipped  = 0;

try {
    $pdo->beginTransaction();

    $handle = fopen($jsonFile, 'r');
    if (!$handle) {
        throw new RuntimeException('Cannot open listeners.json');
    }

    $buffer   = '';
    $level    = 0;
    $inString = false;

    while (($char = fgetc($handle)) !== false) {
        $buffer .= $char;

        if ($char === '"') {
            $inString = !$inString;
        }
        if ($inString) continue;

        if ($char === '{') $level++;
        if ($char === '}') $level--;

        if ($level === 0 && trim($buffer)) {
            $row = json_decode($buffer, true);
            $buffer = '';

            if (!isset($row['song_session_id'], $row['listeners'], $row['recorded_at'])) {
                $skipped++;
                continue;
            }

            $songSessionId = $row['song_session_id'];
            $listeners     = is_numeric($row['listeners']) ? (int)$row['listeners'] : null;



            //$dt = DateTime::createFromFormat('d.m.Y H:i:s', $row['recorded_at']);
            //if (!$dt) {
            //    $skipped++;
            //    continue;
            //}
            //$recordedAt = $dt->format('Y-m-d H:i:s');


            $rawRecordedAt = $row['recorded_at'];
            $dt = null;

            // 1️⃣ ISO 8601 (2025-11-16T16:07:51.317683+01:00)
            try {
                $dt = new DateTime($rawRecordedAt);
            } catch (Exception $e) {
                $dt = false;
            }

            // 2️⃣ fallback: d.m.Y H:i:s
            if (!$dt) {
                $dt = DateTime::createFromFormat('d.m.Y H:i:s', $rawRecordedAt);
            }

            if (!$dt) {
                $skipped++;
                continue;
            }

            $recordedAt = $dt->format('Y-m-d H:i:s');



            // find base play_session
            $getBaseSession->execute([$songSessionId]);
            $base = $getBaseSession->fetch();

            if (!$base) {
                $skipped++;
                continue;
            }

            $insertPlaySession->execute([
                $listeners,
                $songSessionId,
                $base['radio_id'],
                $base['time_id'],
                $recordedAt
            ]);

            $inserted++;
        }
    }

    fclose($handle);
    $pdo->commit();

    $elapsed = microtime(true) - $startTime;
    echo "Listener import completed successfully\n";
    echo "Inserted play_session rows: $inserted\n";
    echo "Skipped records: $skipped\n";
    echo "Execution time: " . round($elapsed, 2) . " seconds";

} catch (Exception $e) {
    $pdo->rollBack();
    echo "Listener import failed: " . $e->getMessage();
}
