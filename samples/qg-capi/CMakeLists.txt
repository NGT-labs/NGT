if(UNIX OR WIN32)
	include_directories("${PROJECT_BINARY_DIR}/lib")
        include_directories("${PROJECT_SOURCE_DIR}/lib")
        link_directories("${PROJECT_SOURCE_DIR}/lib/NGT")

	add_executable(qg-capi qg-capi.cpp)
	add_dependencies(qg-capi NGT::NGT)
	target_link_libraries(qg-capi NGT::NGT Threads::Threads)
endif()
