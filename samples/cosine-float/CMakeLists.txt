if(UNIX OR WIN32)
	include_directories("${PROJECT_BINARY_DIR}/lib")
        include_directories("${PROJECT_SOURCE_DIR}/lib")
        link_directories("${PROJECT_SOURCE_DIR}/lib/NGT")

	add_executable(cosine-float cosine-float.cpp)
	add_dependencies(cosine-float NGT::NGT)
	target_link_libraries(cosine-float NGT::NGT Threads::Threads)
endif()
