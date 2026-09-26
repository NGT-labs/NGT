if(UNIX OR WIN32)
	include_directories("${PROJECT_BINARY_DIR}/lib")
        include_directories("${PROJECT_SOURCE_DIR}/lib")
        link_directories("${PROJECT_SOURCE_DIR}/lib/NGT")

	add_executable(l2-uint8 l2-uint8.cpp)
	add_dependencies(l2-uint8 NGT::NGT)
	target_link_libraries(l2-uint8 NGT::NGT Threads::Threads)
endif()
