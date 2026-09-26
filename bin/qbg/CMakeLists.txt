if(UNIX OR WIN32)
	include_directories("${PROJECT_BINARY_DIR}/lib")
        include_directories("${PROJECT_SOURCE_DIR}/lib")
        link_directories("${PROJECT_SOURCE_DIR}/lib/NGT")

	add_executable(qbg_exe qbg.cpp)
	add_dependencies(qbg_exe NGT::NGT)
        set_target_properties(qbg_exe PROPERTIES OUTPUT_NAME qbg)
	target_link_libraries(qbg_exe NGT::NGT Threads::Threads)

	install(TARGETS qbg_exe RUNTIME DESTINATION bin)

endif()
