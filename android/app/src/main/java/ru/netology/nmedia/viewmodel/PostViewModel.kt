package ru.netology.nmedia.viewmodel

import android.app.Application
import androidx.lifecycle.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import ru.netology.nmedia.db.AppDb
import ru.netology.nmedia.dto.Post
import ru.netology.nmedia.model.FeedModel
import ru.netology.nmedia.model.FeedModelState
import ru.netology.nmedia.repository.PostRepository
import ru.netology.nmedia.repository.PostRepositoryImpl
import ru.netology.nmedia.util.SingleLiveEvent
import kotlin.coroutines.EmptyCoroutineContext

private val empty = Post(
    id = 0,
    content = "",
    author = "",
    authorAvatar = "",
    likedByMe = false,
    likes = 0,
    published = 0,
)

class PostViewModel(application: Application) : AndroidViewModel(application) {
    // упрощённый вариант
    private val _scrollToTop = SingleLiveEvent<Unit>()
    val scrollToTop: LiveData<Unit> = _scrollToTop
    private val repository: PostRepository =
        PostRepositoryImpl(AppDb.getInstance(context = application).postDao())

    val data: LiveData<FeedModel> = repository.data
        .map(::FeedModel)
        .asLiveData(Dispatchers.Default)

    private val _dataState = MutableLiveData<FeedModelState>()
    val dataState: LiveData<FeedModelState>
        get() = _dataState

    val newerCount: LiveData<Int> = data.switchMap {
        repository.getNewerCount(it.posts.firstOrNull()?.id ?: 0L)
            .catch { e -> e.printStackTrace() }
            .asLiveData(Dispatchers.Default)
    }

    private val edited = MutableLiveData(empty)
    private val _postCreated = SingleLiveEvent<Unit>()
    val postCreated: LiveData<Unit>
        get() = _postCreated

    init {
        loadPosts()

    }

    fun showAllPosts() {
        viewModelScope.launch {
            try {
                // 1. Меняем флаги в базе данных
                repository.markAllOldPostsAsNew()

                // 2. КОСТЫЛЬ: Ждем, пока база обновится и UI отрисует изменения
                delay(150L)

                // 3. Отправляем сигнал на скролл
                _scrollToTop.postValue(Unit)

            } catch (e: Exception) {
                // Обработка ошибки остается прежней
                e.printStackTrace()
            }
        }

}
    fun loadPosts() = viewModelScope.launch {
        try {
            _dataState.value = FeedModelState(loading = true)
            repository.fetchAndSaveInitialPosts()
            _dataState.value = FeedModelState()

            // --- ПРОСТОЙ КОСТЫЛЬ ---
            // Ждем полторы секунды (1500 миллисекунд),
            // чтобы RecyclerView точно успел все отрисовать.
            delay(1500L)

            // Теперь отправляем сигнал на скролл
            _scrollToTop.value = Unit
            // ---------------------

        } catch (e: Exception) {
            _dataState.value = FeedModelState(error = true)
        }
    }

    fun refreshPosts() = viewModelScope.launch {
        try {
            _dataState.value = FeedModelState(refreshing = true)
            repository.refreshPosts()
            _dataState.value = FeedModelState()
            delay(100L)
            _scrollToTop.value = Unit
        } catch (e: Exception) {
            _dataState.value = FeedModelState(error = true)
        }
    }


    fun save() {
        edited.value?.let {
            _postCreated.value = Unit
            viewModelScope.launch {
                try {
                    repository.save(it)
                    _dataState.value = FeedModelState()
                } catch (e: Exception) {
                    _dataState.value = FeedModelState(error = true)
                }
            }
        }
        edited.value = empty
    }

    fun edit(post: Post) {
        edited.value = post
    }

    fun changeContent(content: String) {
        val text = content.trim()
        if (edited.value?.content == text) {
            return
        }
        edited.value = edited.value?.copy(content = text)
    }

    fun likeById(id: Long) {

        viewModelScope.launch {
            try {
                _dataState.value = FeedModelState(loading = true)
                repository.likeById(id)
                _dataState.value = FeedModelState()
            } catch (e: Exception) {
                _dataState.value = FeedModelState(error = true)
            }
        }
    }

    fun removeById(id: Long) {
        viewModelScope.launch {
            try {
                _dataState.value = FeedModelState(loading = true)
                repository.removeById(id)
                _dataState.value = FeedModelState()
            } catch (e: Exception) {
                _dataState.value = FeedModelState(error = true)
            }
        }
    }
}
