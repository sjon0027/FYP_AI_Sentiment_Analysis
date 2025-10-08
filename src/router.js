import { createRouter, createWebHistory } from 'vue-router'
import AboutPage from './pages/AboutPage.vue'
import ContactPage from './pages/ContactPage.vue'
import Semester1 from './pages/Semester1.vue'
import Dashboard from './pages/Dashboard.vue'
import Testing from './pages/Testing.vue'

const routes = [
  { path: '/FYP_2025/', component: Dashboard },
  { path: '/FYP_2025/about', component: AboutPage },
  { path: '/FYP_2025/contact', component: ContactPage },
  { path: '/FYP_2025/fit4701', component: Semester1 },
  { path: '/FYP_2025/test', component: Testing },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

export default router
