package ru.netology.nmedia.dao

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import kotlinx.coroutines.flow.Flow
import ru.netology.nmedia.entity.PostEntity

@Dao
interface PostDao {
    @Query("SELECT * FROM PostEntity WHERE isNew = 1 ORDER BY id DESC")
    fun getAll(): Flow<List<PostEntity>>
    @Query("SELECT MAX(id) FROM PostEntity")
    suspend fun getLastPostId(): Long
    @Query("UPDATE PostEntity SET isNew = 1 WHERE isNew = 0")
    suspend fun markAllOldPostsAsNew()
    @Query("SELECT COUNT(*) == 0 FROM PostEntity")
    suspend fun isEmpty(): Boolean
    @Query("SELECT id FROM PostEntity")
    suspend fun getAllPostIds(): List<Long>
    @Query("SELECT COUNT(*) FROM PostEntity")
    suspend fun count(): Int

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insert(post: PostEntity)

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insert(posts: List<PostEntity>)

    @Query("DELETE FROM PostEntity WHERE id = :id")
    suspend fun removeById(id: Long)

    @Query("SELECT COUNT(*) FROM PostEntity WHERE id > :lastLocalPostId")
    suspend fun getNewPostsCount(lastLocalPostId: Long): Int


}
