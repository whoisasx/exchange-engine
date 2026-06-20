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
  src/runtime/EngineOutbox.cpp
  src/runtime/EngineOutputSerializer.cpp
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

add_library(engine_checkpoint
  src/checkpoint/EngineCheckpointManager.cpp
  src/checkpoint/InMemoryCheckpointStore.cpp
)
add_library(cex::engine_checkpoint ALIAS engine_checkpoint)

target_compile_features(engine_checkpoint PUBLIC cxx_std_20)

target_include_directories(engine_checkpoint
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(engine_checkpoint
  PUBLIC
    cex::engine_runtime
)

add_library(engine_broker
  src/broker/RedpandaEngineApp.cpp
)
add_library(cex::engine_broker ALIAS engine_broker)

target_compile_features(engine_broker PUBLIC cxx_std_20)

target_include_directories(engine_broker
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(engine_broker
  PUBLIC
    cex::engine_runtime
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

  add_executable(checkpoint_tests
    tests/checkpoint/CheckpointTests.cpp
  )

  target_link_libraries(checkpoint_tests
    PRIVATE
      cex::engine_checkpoint
  )

  add_test(NAME checkpoint_tests COMMAND checkpoint_tests)

  add_executable(broker_tests
    tests/broker/RedpandaEngineAppTests.cpp
  )

  target_link_libraries(broker_tests
    PRIVATE
      cex::engine_broker
  )

  target_compile_definitions(broker_tests
    PRIVATE
      PROTOCOL_EXAMPLES_DIR="${CMAKE_CURRENT_SOURCE_DIR}/docs/examples"
  )

  add_test(NAME broker_tests COMMAND broker_tests)

  add_executable(service_contract_tests
    tests/services/ServiceContractsTest.cpp
  )

  target_link_libraries(service_contract_tests
    PRIVATE
      cex::services
  )

  add_test(NAME service_contract_tests COMMAND service_contract_tests)
endif()
