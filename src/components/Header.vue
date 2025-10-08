<template>
  <div class="max-w-7xl mx-auto flex items-center justify-between p-4 bg-deepaqua">
    <!-- Logo + Title -->
    <div class="flex items-center space-x-3">
      <!-- <img src="your-logo.png" alt="Logo" class="w-10 h-10 rounded-full" /> -->
      <h1 class="text-xl font-bold">FIT4701/4702 FYP Project Monash</h1>
    </div>

    <!-- Navigation -->
    <nav class="space-x-6 hidden sm:block">
      <router-link to="/FYP_2025/" class="btn">Dashboard</router-link>
      <router-link to="/FYP_2025/test" class="btn">Testing</router-link>
      <router-link to="/FYP_2025/fit4701" class="btn">FIT4701</router-link>
      <router-link to="/FYP_2025/about" class="btn">About</router-link>
      <router-link to="/FYP_2025/contact" class="btn">Contact</router-link>
    </nav>
  </div>
</template>

<script setup>
import { ref } from 'vue'
</script>
