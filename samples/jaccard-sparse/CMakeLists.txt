if(UNIX OR WIN32)
	include_directories("${PROJECT_BINARY_DIR}/lib")
        include_directories("${PROJECT_SOURCE_DIR}/lib")
        link_directories("${PROJECT_SOURCE_DIR}/lib/NGT")

	add_executable(jaccard-sparse jaccard-sparse.cpp)
	add_dependencies(jaccard-sparse ngt)
	target_link_libraries(jaccard-sparse ngt Threads::Threads)
endif()
