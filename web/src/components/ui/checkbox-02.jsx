import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Check } from 'lucide-react';

// Reusable Premium Checkbox Component
export const PremiumCheckbox = ({
  id,
  label,
  description,
  checked,
  onChange,
  disabled = false
}) => {
  return (
    <div className="relative">
      <label
        htmlFor={id}
        className={`flex items-start gap-6 cursor-pointer group ${
          disabled ? 'opacity-50 cursor-not-allowed' : ''
        }`}
      >
        {/* Checkbox Container */}
        <div className="relative flex items-center justify-center mt-1">
          {/* Hidden native checkbox for accessibility */}
          <input
            id={id}
            type="checkbox"
            checked={checked}
            onChange={onChange}
            disabled={disabled}
            className="sr-only"
          />

          {/* Custom checkbox visual */}
          <motion.div
            className={`
              w-7 h-7 rounded-lg border-2 flex items-center justify-center
              transition-colors duration-300
              ${
                checked
                  ? 'bg-white border-white'
                  : 'bg-black border-gray-700 group-hover:border-gray-500'
              }
            `}
            whileHover={!disabled ? { scale: 1.05 } : {}}
            whileTap={!disabled ? { scale: 0.95 } : {}}
          >
            <AnimatePresence mode="wait">
              {checked && (
                <motion.div
                  initial={{ scale: 0, opacity: 0 }}
                  animate={{ scale: 1, opacity: 1 }}
                  exit={{ scale: 0, opacity: 0 }}
                  transition={{
                    type: "spring",
                    stiffness: 500,
                    damping: 25
                  }}
                >
                  <Check className="w-5 h-5 text-black stroke-[3]" />
                </motion.div>
              )}
            </AnimatePresence>
          </motion.div>

          {/* Glow effect on hover */}
          {!disabled && (
            <motion.div
              className="absolute inset-0 rounded-lg bg-white opacity-0 group-hover:opacity-10 transition-opacity duration-300"
              style={{ filter: 'blur(8px)' }}
            />
          )}
        </div>

        {/* Label and Description */}
        <div className="flex-1 space-y-2">
          <div className="text-white font-medium text-lg tracking-wide">
            {label}
          </div>
          {description && (
            <div className="text-gray-400 text-sm leading-relaxed">
              {description}
            </div>
          )}
        </div>
      </label>
    </div>
  );
};

// Main Demo Component
export default function App() {
  const [checkboxes, setCheckboxes] = useState({
    notifications: true,
    marketing: false,
    analytics: true,
    performance: false,
  });

  const handleCheckboxChange = (key) => {
    setCheckboxes(prev => ({ ...prev, [key]: !prev[key] }));
  };

  const options = [
    {
      id: 'notifications',
      label: 'Push Notifications',
      description: 'Receive instant updates about important activities and mentions across your workspace.',
    },
    {
      id: 'marketing',
      label: 'Marketing Communications',
      description: 'Get curated insights, product updates, and exclusive offers delivered to your inbox.',
    },
    {
      id: 'analytics',
      label: 'Analytics & Performance',
      description: 'Allow us to collect anonymous usage data to enhance your experience and improve our platform.',
    },
    {
      id: 'performance',
      label: 'Performance Tracking',
      description: 'Enable advanced performance monitoring and detailed usage statistics for optimization.',
    },
  ];

  return (
    <div className="w-full relative min-h-screen bg-black text-white">
      <div className="min-h-screen flex items-center justify-center p-12">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, ease: [0.22, 1, 0.36, 1] }}
          className="w-full max-w-3xl"
        >
          {/* Header Section */}
          <div className="mb-20 space-y-6">
            <motion.h1
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2, duration: 0.8 }}
              className="text-6xl font-light tracking-tight"
            >
              Preferences
            </motion.h1>
            <motion.p
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.3, duration: 0.8 }}
              className="text-gray-400 text-lg leading-relaxed max-w-2xl"
            >
              Customize your experience with granular control over notifications,
              communications, and data collection preferences.
            </motion.p>
          </div>

          {/* Checkboxes */}
          <div className="space-y-12">
            {options.map((option, index) => (
              <motion.div
                key={option.id}
                initial={{ opacity: 0, x: -20 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{
                  delay: 0.4 + index * 0.1,
                  duration: 0.6,
                  ease: [0.22, 1, 0.36, 1]
                }}
              >
                <PremiumCheckbox
                  id={option.id}
                  label={option.label}
                  description={option.description}
                  checked={checkboxes[option.id]}
                  onChange={() => handleCheckboxChange(option.id)}
                />
              </motion.div>
            ))}
          </div>

          {/* Footer Action */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 1, duration: 0.8 }}
            className="mt-20 flex justify-end"
          >
            <motion.button
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
              className="px-10 py-4 bg-white text-black font-medium rounded-lg tracking-wide hover:bg-gray-100 transition-colors"
            >
              Save Preferences
            </motion.button>
          </motion.div>
        </motion.div>
      </div>
    </div>
  );
}
