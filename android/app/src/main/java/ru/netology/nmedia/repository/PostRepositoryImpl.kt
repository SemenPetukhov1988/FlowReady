package ru.netology.nmedia.repository

import androidx.lifecycle.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import ru.netology.nmedia.api.*
import ru.netology.nmedia.dao.PostDao
import ru.netology.nmedia.dto.Post
import ru.netology.nmedia.entity.PostEntity
import ru.netology.nmedia.entity.toDto
import ru.netology.nmedia.entity.toEntity
import ru.netology.nmedia.error.ApiError
import ru.netology.nmedia.error.AppError
import ru.netology.nmedia.error.NetworkError
import ru.netology.nmedia.error.UnknownError
import java.io.IOException

class PostRepositoryImpl(private val dao: PostDao) : PostRepository {
    override val data = dao.getAll()
        .map(List<PostEntity>::toDto)
        .flowOn(Dispatchers.Default)

    override suspend fun fetchAndSaveInitialPosts() {
        val response = PostsApi.service.getAll()
        if (response.isSuccessful) {
            val serverPosts = response.body() ?: return

            // 1. Получаем список всех ID, которые уже есть в нашей локальной базе
            val existingPostIds = dao.getAllPostIds().toSet() // Используем Set для быстрого поиска

            // 2. Фильтруем посты с сервера: берем только те, которых еще нет в базе
            val newPostsFromServer = serverPosts.filter { !existingPostIds.contains(it.id) }

            // 3. Если на сервере есть новые посты, которых нет у нас
            if (newPostsFromServer.isNotEmpty()) {
                // Находим самый большой ID среди этих новых постов
                // Это будет наш "порог" для определения "новизны" для пользователя
                val maxNewServerId = newPostsFromServer.maxOf { it.id }

                // 4. Трансформируем ТОЛЬКО новые посты
                val entitiesToInsert = newPostsFromServer.map { postDto ->
                    // Для новых постов флаг будет зависеть от их ID
                    val isPostNewForUser = postDto.id > maxNewServerId

                    // Используем существующий fromDto и меняем флаг
                    PostEntity.fromDto(postDto).copy(isNew = isPostNewForUser)
                }

                // 5. Вставляем в базу ТОЛЬКО новые сущности
                dao.insert(entitiesToInsert)
            } else {
                // Если новых постов нет, ничего не делаем.
                // База остается в прежнем состоянии.
            }
        } else {
            throw ApiError(response.code(), response.message())
        }
    }

    override suspend fun refreshPosts() {

            val response = PostsApi.service.getAll()
            if (response.isSuccessful) {
                val serverPosts = response.body() ?: return

                // Получаем список всех ID, которые УЖЕ есть в нашей локальной базе
                val existingPostIds = dao.getAllPostIds()

                // Фильтруем посты с сервера: берем только те, которых еще нет в базе
                val newPostsToAdd = serverPosts.filter { !existingPostIds.contains(it.id) }

                // Конвертируем их в Entity.
                // Так как это НОВЫЕ посты (их нет в базе), мы можем смело ставить им isNew = false,
                // чтобы они не появлялись в ленте автоматически.
                val entitiesToInsert = newPostsToAdd.map {
                    PostEntity.fromDto(it).copy(isNew = false)
                }

                if (entitiesToInsert.isNotEmpty()) {
                    dao.insert(entitiesToInsert)
                }
            } else {
                throw ApiError(response.code(), response.message())
            }
        }


    override suspend fun markAllOldPostsAsNew() {
        dao.markAllOldPostsAsNew()
    }


    override suspend fun getAll() {
        try {
            val response = PostsApi.service.getAll()
            if (!response.isSuccessful) {
                throw ApiError(response.code(), response.message())
            }

            val body = response.body() ?: throw ApiError(response.code(), response.message())
            dao.insert(body.toEntity())
        } catch (e: IOException) {
            throw NetworkError
        } catch (e: Exception) {
            throw UnknownError
        }
    }

    override fun getNewerCount(id: Long): Flow<Int> = flow {
        while (true) {
            delay(10_000L)
            val response = PostsApi.service.getNewer(id)
            if (!response.isSuccessful) {
                throw ApiError(response.code(), response.message())
            }

            val body = response.body() ?: throw ApiError(response.code(), response.message())
           dao.insert(body.toEntity())
            emit(body.size)
        }
    }
        .catch { e -> throw AppError.from(e) }
        .flowOn(Dispatchers.Default)

    override suspend fun save(post: Post) {
        try {
            val response = PostsApi.service.save(post)
            if (!response.isSuccessful) {
                throw ApiError(response.code(), response.message())
            }

            val body = response.body() ?: throw ApiError(response.code(), response.message())
            dao.insert(PostEntity.fromDto(body))
        } catch (e: IOException) {
            throw NetworkError
        } catch (e: Exception) {
            throw UnknownError
        }
    }

    override suspend fun removeById(id: Long) {
        TODO("Not yet implemented")
    }

    override suspend fun likeById(id: Long) {
        TODO("Not yet implemented")
    }
}
