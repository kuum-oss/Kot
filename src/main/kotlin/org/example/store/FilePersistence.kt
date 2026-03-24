package org.example.store

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch
import org.example.events.Event
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardOpenOption

class FilePersistence(
    private val filePath: String,
    private val scope: CoroutineScope
) {
    private val logger = LoggerFactory.getLogger(FilePersistence::class.java)
    private val persistChannel = Channel<Event>(Channel.UNLIMITED)
    private val file = File(filePath)

    init {
        if (!file.exists()) {
            file.createNewFile()
        }
        
        scope.launch(Dispatchers.IO) {
            for (event in persistChannel) {
                try {
                    // Simple CSV-like format for events to avoid heavy JSON dependencies if not needed
                    // In real world, use Protobuf or Avro
                    val line = "${event.javaClass.simpleName}|${event.aggregateId}|${event.id}|${event.timestamp}\n"
                    Files.write(file.toPath(), line.toByteArray(), StandardOpenOption.APPEND)
                } catch (e: Exception) {
                    logger.error("Failed to persist event", e)
                }
            }
        }
    }

    suspend fun persist(event: Event) {
        persistChannel.send(event)
    }
}
