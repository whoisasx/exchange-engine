cmake_minimum_required(VERSION 3.20)

project(core_engine
  VERSION 0.1.0
  LANGUAGES CXX
)

set(CORE_ENGINE_SOURCES
  src/core/EngineCore.cpp
  src/core/FarPriceMap.cpp
  src/core/FixedPoint.cpp
  src/core/IdempotencyIndex.cpp
  src/core/OrderBook.cpp
  src/core/PriceLevel.cpp
  src/core/RingPriceLadder.cpp
  src/core/SequenceGenerator.cpp
  src/core/SideBook.cpp
  src/core/Validation.cpp
)

add_library(core_engine STATIC ${CORE_ENGINE_SOURCES})
add_library(cex::core_engine ALIAS core_engine)

target_compile_features(core_engine PUBLIC cxx_std_20)

target_include_directories(core_engine
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/core
)

add_library(core_services
  src/services/FakeEngineSmoke.cpp
  src/services/Ledger.cpp
  src/services/OrderbookProjector.cpp
  src/services/Timeseries.cpp
  src/services/WebsocketFanout.cpp
)
add_library(cex::services ALIAS core_services)

target_compile_features(core_services PUBLIC cxx_std_20)

target_include_directories(core_services
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

add_library(engine_adapter
  src/adapter/EngineAdapter.cpp
)
add_library(cex::engine_adapter ALIAS engine_adapter)

target_compile_features(engine_adapter PUBLIC cxx_std_20)

target_include_directories(engine_adapter
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(engine_adapter
  PUBLIC
    cex::core_engine
)

add_library(engine_protocol
  src/protocol/Json.cpp
  src/protocol/Message.cpp
)
add_library(cex::engine_protocol ALIAS engine_protocol)

target_compile_features(engine_protocol PUBLIC cxx_std_20)

target_include_directories(engine_protocol
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

add_library(engine_runtime
  src/runtime/EngineEventTranslator.cpp
  src/runtime/EngineInputParser.cpp
  src/runtime/EngineRuntime.cpp
)
add_library(cex::engine_runtime ALIAS engine_runtime)

target_compile_features(engine_runtime PUBLIC cxx_std_20)

target_include_directories(engine_runtime
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(engine_runtime
  PUBLIC
    cex::core_engine
    cex::engine_adapter
    cex::engine_protocol
)

include(CTest)

if(BUILD_TESTING)
  add_executable(adapter_tests
    tests/adapter/AdapterTests.cpp
  )

  target_link_libraries(adapter_tests
    PRIVATE
      cex::engine_adapter
  )

  add_test(NAME adapter_tests COMMAND adapter_tests)

  add_executable(protocol_fixture_tests
    tests/protocol/protocol_fixture_tests.cpp
  )

  target_link_libraries(protocol_fixture_tests
    PRIVATE
      cex::engine_protocol
  )

  target_compile_definitions(protocol_fixture_tests
    PRIVATE
      PROTOCOL_EXAMPLES_DIR="${CMAKE_CURRENT_SOURCE_DIR}/docs/examples"
  )

  add_test(
    NAME protocol_fixture_tests
    COMMAND protocol_fixture_tests
  )

  add_executable(runtime_tests
    tests/runtime/RuntimePlaceOrderTests.cpp
  )

  target_link_libraries(runtime_tests
    PRIVATE
      cex::engine_runtime
  )

  target_compile_definitions(runtime_tests
    PRIVATE
      PROTOCOL_EXAMPLES_DIR="${CMAKE_CURRENT_SOURCE_DIR}/docs/examples"
  )

  add_test(NAME runtime_tests COMMAND runtime_tests)

  add_executable(service_contract_tests
    tests/services/ServiceContractsTest.cpp
  )

  target_link_libraries(service_contract_tests
    PRIVATE
      cex::services
  )

  add_test(NAME service_contract_tests COMMAND service_contract_tests)
endif()
