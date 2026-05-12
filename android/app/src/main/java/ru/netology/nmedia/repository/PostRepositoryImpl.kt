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

            // Получаем список всех ID, которые уже есть в нашей локальной базе
            val existingPostIds = dao.getAllPostIds().toSet()

            // 1. Находим посты с сервера, которых еще нет у нас (это "новые данные")
            val newPostsFromServer = serverPosts.filter { !existingPostIds.contains(it.id) }

            // 2. Определяем "порог новизны" для пользователя.
            // Это самый большой ID из тех постов, что УЖЕ были в базе.
            // Если база была пуста (как при первом запуске), то lastLocalPostId будет 0 или null.
            val lastLocalPostId = dao.getLastPostId()


            // Мы будем помечать как "новые" только если база была ПУСТАЯ.
            // Это означает первый запуск приложения.
            val isFirstLaunch = lastLocalPostId == 0L || existingPostIds.isEmpty()


            // Если есть новые посты для загрузки ИЛИ это первый запуск (чтобы пометить все как новые)
            if (newPostsFromServer.isNotEmpty() || isFirstLaunch) {

                // Создаем список сущностей для вставки
                val entitiesToInsert = if (isFirstLaunch) {
                    // Если это первый запуск, помечаем ВСЕ пришедшие посты как новые
                    serverPosts.map { PostEntity.fromDto(it).copy(isNew = true) }
                } else {
                    // Если это не первый запуск (обновление), помечаем только НОВЫЕ посты как "не новые"
                    newPostsFromServer.map { PostEntity.fromDto(it).copy(isNew = false) }
                }

                dao.insert(entitiesToInsert)

                // --- ДОПОЛНИТЕЛЬНО: Обработка старых постов ---
                // Если это не первый запуск, нам нужно проверить старые посты,
                // которые стали "новыми" для пользователя (их ID больше старого порога)
                if (!isFirstLaunch) {
                    val oldPostsToMarkAsNew = existingPostIds.filter { it > lastLocalPostId }
                    if (oldPostsToMarkAsNew.isNotEmpty()) {
                        dao.updateIsNewByIds(oldPostsToMarkAsNew, true)
                    }
                }

            } // Конец условия if
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

        dao.removeById(id)

        try {
            val response = PostsApi.service.removeById(id)
            if (!response.isSuccessful) {
                throw ApiError(response.code(), response.message())
            }
        } catch (e: Exception) {
            throw e
        }
    }

    override suspend fun likeById(id: Long) {
        dao.likeById(id)
        try {
            val post = dao.getPostById(id)
            if (post.likedByMe) {
                PostsApi.service.likeById(id)
            } else {
                PostsApi.service.dislikeById(id)
            }
        } catch (e: Exception) {
            dao.likeById(id)
            throw e
        }
    }
}
