/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __WARABI_ADMIN_HPP
#define __WARABI_ADMIN_HPP

#include <nlohmann/json.hpp>
#include <thallium.hpp>
#include <string>
#include <memory>
#include <warabi/Exception.hpp>
#include <warabi/UUID.hpp>

namespace warabi {

namespace tl = thallium;

class AdminImpl;

/**
 * @brief Admin interface to a WARABI service. Enables creating
 * and destroying targets, and attaching and detaching them
 * from a provider. If WARABI providers have set up a security
 * token, operations from the Admin interface will need this
 * security token.
 */
class Admin {

    public:

    using json = nlohmann::json;

    /**
     * @brief Default constructor.
     */
    Admin();

    /**
     * @brief Constructor using a margo instance id.
     *
     * @param mid Margo instance id.
     */
    Admin(margo_instance_id mid);

    /**
     * @brief Constructor.
     *
     * @param engine Thallium engine.
     */
    Admin(const tl::engine& engine);

    /**
     * @brief Copy constructor.
     */
    Admin(const Admin&);

    /**
     * @brief Move constructor.
     */
    Admin(Admin&&);

    /**
     * @brief Copy-assignment operator.
     */
    Admin& operator=(const Admin&);

    /**
     * @brief Move-assignment operator.
     */
    Admin& operator=(Admin&&);

    /**
     * @brief Destructor.
     */
    ~Admin();

    /**
     * @brief Check if the Admin instance is valid.
     */
    operator bool() const;

    /**
     * @brief Adds a target on the target provider.
     * The config string must be a JSON object acceptable
     * by the desired backend's creation function.
     *
     * @param address Address of the target provider.
     * @param provider_id Provider id.
     * @param type Type of the target to create.
     * @param config JSON configuration for the target.
     */
    UUID addTarget(const std::string& address,
                   uint16_t provider_id,
                   const std::string& type,
                   const std::string& config,
                   const std::string& token="") const;

    /**
     * @brief Adds a target on the target provider.
     * The config string must be a JSON object acceptable
     * by the desired backend's creation function.
     *
     * @param address Address of the target provider.
     * @param provider_id Provider id.
     * @param type Type of the target to create.
     * @param config JSON configuration for the target.
     */
    UUID addTarget(const std::string& address,
                   uint16_t provider_id,
                   const std::string& type,
                   const char* config,
                   const std::string& token="") const {
        return addTarget(address, provider_id, type, std::string(config), token);
    }

    /**
     * @brief Adds a target on the target provider.
     * The config object must be a JSON object acceptable
     * by the desired backend's creation function.
     *
     * @param address Address of the target provider.
     * @param provider_id Provider id.
     * @param type Type of the target to create.
     * @param config JSON configuration for the target.
     */
    UUID addTarget(const std::string& address,
                      uint16_t provider_id,
                      const std::string& type,
                      const json& config,
                      const std::string& token="") const {
        return addTarget(address, provider_id, type, config.dump(), token);
    }

    /**
     * @brief Removes an open target in the target provider.
     *
     * @param address Address of the target provider.
     * @param provider_id Provider id.
     * @param target_id UUID of the target to close.
     */
    void removeTarget(const std::string& address,
                      uint16_t provider_id,
                      const UUID& target_id,
                      const std::string& token="") const;

    /**
     * @brief Destroys an open target in the target provider.
     *
     * @param address Address of the target provider.
     * @param provider_id Provider id.
     * @param target_id UUID of the target to destroy.
     */
    void destroyTarget(const std::string& address,
                       uint16_t provider_id,
                       const UUID& target_id,
                       const std::string& token="") const;

    private:

    std::shared_ptr<AdminImpl> self;
};

}

#endif
