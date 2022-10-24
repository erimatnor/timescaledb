# Add all *.c to sources in upperlevel directory
#set(OPENTELEMETRY_C_SOURCES
#  ${CMAKE_CURRENT_SOURCE_DIR}/opentelemetry-c.cpp)

add_library(opentelemetry-c STATIC opentelemetry-c.cpp)
##set_target_properties(opentelemetry-c PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(opentelemetry-c SYSTEM PUBLIC ${OPENTELEMETRY_CPP_INCLUDE_DIR})
target_link_libraries(opentelemetry-c PUBLIC ${OPENTELEMETRY_CPP_LIBRARIES})
